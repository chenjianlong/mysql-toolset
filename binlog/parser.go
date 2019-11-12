//
// parser.go
// Copyright (C) 2019 Jianlong Chen <jianlong99@gmail.com>
//

package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
)

// log event type, from libbinlogevents/include/binlog_event.h
const (
	UNKNOWN_EVENT  = 0
	START_EVENT_V3 = 1 // Deprecated since mysql 8.0.2.
	QUERY_EVENT    = 2
	STOP_EVENT     = 3
	ROTATE_EVENT   = 4
	INTVAR_EVENT   = 5

	SLAVE_EVENT = 7

	APPEND_BLOCK_EVENT = 9
	DELETE_FILE_EVENT  = 11

	RAND_EVENT               = 13
	USER_VAR_EVENT           = 14
	FORMAT_DESCRIPTION_EVENT = 15
	XID_EVENT                = 16
	BEGIN_LOAD_QUERY_EVENT   = 17
	EXECUTE_LOAD_QUERY_EVENT = 18

	TABLE_MAP_EVENT = 19

	// The V1 event numbers are used from 5.1.16 until mysql-5.6.
	WRITE_ROWS_EVENT_V1  = 23
	UPDATE_ROWS_EVENT_V1 = 24
	DELETE_ROWS_EVENT_V1 = 25

	// Something out of the ordinary happened on the master
	INCIDENT_EVENT = 26

	/**
	Heartbeat event to be send by master at its idle time
	to ensure master's online status to slave
	*/
	HEARTBEAT_LOG_EVENT = 27

	/**
	In some situations, it is necessary to send over ignorable
	data to the slave: data that a slave can handle in case there
	is code for handling it, but which can be ignored if it is not
	recognized.
	*/
	IGNORABLE_LOG_EVENT  = 28
	ROWS_QUERY_LOG_EVENT = 29

	/** Version 2 of the Row events */
	WRITE_ROWS_EVENT  = 30
	UPDATE_ROWS_EVENT = 31
	DELETE_ROWS_EVENT = 32

	GTID_LOG_EVENT           = 33
	ANONYMOUS_GTID_LOG_EVENT = 34

	PREVIOUS_GTIDS_LOG_EVENT = 35

	TRANSACTION_CONTEXT_EVENT = 36

	VIEW_CHANGE_EVENT = 37

	/* Prepared XA transaction terminal event similar to Xid */
	XA_PREPARE_LOG_EVENT = 38

	/**
	Extension of UPDATE_ROWS_EVENT, allowing partial values according
	to binlog_row_value_options.
	*/
	PARTIAL_UPDATE_ROWS_EVENT = 39
)

const (
	BINLOG_EVENT_HEADER_LEN = 19
)

type BinLogEventHeader struct {
	Timestamp uint32 //  seconds since unix epoch
	EventType uint8
	ServerId  uint32 // server-id of the originating mysql-server. Used to filter out events in circular replication.
	EventSize uint32 // size of the event (header, post-header, body)
	LogPos    uint32 // position of the next event
	Flags     uint16
}

func (header *BinLogEventHeader) String() string {
	return fmt.Sprintf("{ timestamp: %d, eventType: %d, serverId: %d, eventSize: %d, logPos: %d, flags: %d }",
		header.Timestamp, header.EventType, header.ServerId, header.EventSize, header.LogPos, header.Flags)
}

func NewBinLogEventHeader(text []byte) (*BinLogEventHeader, error) {
	if len(text) != BINLOG_EVENT_HEADER_LEN {
		panic("Invalid binlog event header")
	}

	reader := bytes.NewReader(text)
	header := new(BinLogEventHeader)
	err := binary.Read(reader, binary.LittleEndian, header)
	return header, err
}

type BinLogEvent interface {
	GetHeader() string
	GetPostHeader() string
	GetPayload() string
}

type UnknownBinLogEvent struct {
	header *BinLogEventHeader
}

func (event *UnknownBinLogEvent) GetHeader() string {
	return event.header.String()
}

func (event *UnknownBinLogEvent) GetPostHeader() string {
	return "(empty)"
}

func (event *UnknownBinLogEvent) GetPayload() string {
	return "(empty)"
}

type FormatDescriptionEventPayload struct {
	BinlogVersion         uint16 // version of this binlog format
	MySQLServerVersion    []byte // version of the MySQL Server that created the binlog
	CreateTimestamp       uint32 // seconds since Unix epoch when the binlog was created
	EventHeaderLength     uint8  // length of the Binlog Event Header of next events. Should always be 19
	EventTypeHeaderLength []byte // a array indexed by Binlog Event Type - 1 to extract the length of the event specific header
}

func (payload *FormatDescriptionEventPayload) String() string {
	return fmt.Sprintf(
		"{ binlog_version: %d, mysql_server_version: %s,"+
			" create_timestamp: %d, event_header_length: %d, event_type_header_length: %v }",
		payload.BinlogVersion, payload.MySQLServerVersion,
		payload.CreateTimestamp, payload.EventHeaderLength,
		payload.EventTypeHeaderLength)
}

type FormatDescriptionEvent struct {
	header  *BinLogEventHeader
	payload *FormatDescriptionEventPayload
}

func (event *FormatDescriptionEvent) GetHeader() string {
	return event.header.String()
}

func (event *FormatDescriptionEvent) GetPostHeader() string {
	return "(empty)"
}

func (event *FormatDescriptionEvent) GetPayload() string {
	return event.payload.String()
}

func newFormatDescriptionEventPayload(
	header *BinLogEventHeader, text []byte) (*FormatDescriptionEventPayload, error) {

	size := header.EventSize - BINLOG_EVENT_HEADER_LEN
	if size != uint32(len(text)) {
		panic("Invalid FormatDescriptionEventPayload len")
	}

	r := bytes.NewReader(text)
	payload := new(FormatDescriptionEventPayload)
	payload.MySQLServerVersion = make([]byte, 50)
	payload.EventTypeHeaderLength = make([]byte, size-(2+50+4+1))
	err := binary.Read(r, binary.LittleEndian, payload.BinlogVersion)
	if err != nil {
		return nil, err
	}

	if err = binary.Read(r, binary.LittleEndian, payload.MySQLServerVersion); err != nil {
		return nil, err
	}

	if err = binary.Read(r, binary.LittleEndian, (*payload).CreateTimestamp); err != nil {
		return nil, err
	}

	if err = binary.Read(r, binary.LittleEndian, payload.EventHeaderLength); err != nil {
		return nil, err
	}

	if err = binary.Read(r, binary.LittleEndian, payload.EventTypeHeaderLength); err != nil {
		return nil, err
	}

	return payload, err
}

func NewBinLogEvent(header *BinLogEventHeader, text []byte) BinLogEvent {
	switch header.EventType {
	case FORMAT_DESCRIPTION_EVENT:
		payload, err := newFormatDescriptionEventPayload(header, text)
		if err != nil {
			panic(err)
		}

		return &FormatDescriptionEvent{header, payload}
	default:
		return &UnknownBinLogEvent{header}
	}
}

func usage() {
	fmt.Printf("Usage: %s <binlog>\n", os.Args[0])
	os.Exit(2)
}

func readEventBody(file *os.File, header *BinLogEventHeader, text *[]byte) error {
	size := header.EventSize - BINLOG_EVENT_HEADER_LEN
	if size == 0 {
		return nil
	}

	*text = (*text)[0:size]
	n, err := file.Read(*text)
	if err != nil {
		return err
	}

	if n != len(*text) {
		panic("Failed to read event body")
	}

	return nil
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}

	file, err := os.Open(os.Args[1])
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	defer file.Close()

	text := make([]byte, 4, 1024*1024)
	var n int
	n, err = file.Read(text)
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	if n != 4 {
		panic("Failed to read binlog file header")
	}

	if text[0] != 0xfe || text[1] != 'b' || text[2] != 'i' || text[3] != 'n' {
		panic("Invalid binlog file header")
	}

	text = text[0:BINLOG_EVENT_HEADER_LEN]
	n, err = file.Read(text)
	if n != len(text) {
		panic("Failed to read binlog event header")
	}

	header, err := NewBinLogEventHeader(text)
	if err != nil {
		panic(err)
	}

	if err = readEventBody(file, header, &text); err != nil {
		panic(err)
	}

	event := NewBinLogEvent(header, text)
	fmt.Printf("header: %s\npostHeader: %s\npayload: %s\n",
		event.GetHeader(), event.GetPostHeader(), event.GetPayload())
}
