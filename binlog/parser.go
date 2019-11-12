//
// parser.go
// Copyright (C) 2019 Jianlong Chen <jianlong99@gmail.com>
//

package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/alexflint/go-arg"
	"io"
	"os"
)

// log event type, from libbinlogevents/include/binlog_event.h
type LogEventType uint8

const (
	UNKNOWN_EVENT  LogEventType = 0
	START_EVENT_V3 LogEventType = 1 // Deprecated since mysql 8.0.2.
	QUERY_EVENT    LogEventType = 2
	STOP_EVENT     LogEventType = 3
	ROTATE_EVENT   LogEventType = 4
	INTVAR_EVENT   LogEventType = 5

	SLAVE_EVENT LogEventType = 7

	APPEND_BLOCK_EVENT LogEventType = 9
	DELETE_FILE_EVENT  LogEventType = 11

	RAND_EVENT               LogEventType = 13
	USER_VAR_EVENT           LogEventType = 14
	FORMAT_DESCRIPTION_EVENT LogEventType = 15
	XID_EVENT                LogEventType = 16
	BEGIN_LOAD_QUERY_EVENT   LogEventType = 17
	EXECUTE_LOAD_QUERY_EVENT LogEventType = 18

	TABLE_MAP_EVENT LogEventType = 19

	// The V1 event numbers are used from 5.1.16 until mysql-5.6.
	WRITE_ROWS_EVENT_V1  LogEventType = 23
	UPDATE_ROWS_EVENT_V1 LogEventType = 24
	DELETE_ROWS_EVENT_V1 LogEventType = 25

	// Something out of the ordinary happened on the master
	INCIDENT_EVENT LogEventType = 26

	/**
	Heartbeat event to be send by master at its idle time
	to ensure master's online status to slave
	*/
	HEARTBEAT_LOG_EVENT LogEventType = 27

	/**
	In some situations, it is necessary to send over ignorable
	data to the slave: data that a slave can handle in case there
	is code for handling it, but which can be ignored if it is not
	recognized.
	*/
	IGNORABLE_LOG_EVENT  LogEventType = 28
	ROWS_QUERY_LOG_EVENT LogEventType = 29

	/** Version 2 of the Row events */
	WRITE_ROWS_EVENT  LogEventType = 30
	UPDATE_ROWS_EVENT LogEventType = 31
	DELETE_ROWS_EVENT LogEventType = 32

	GTID_LOG_EVENT           LogEventType = 33
	ANONYMOUS_GTID_LOG_EVENT LogEventType = 34

	PREVIOUS_GTIDS_LOG_EVENT LogEventType = 35

	TRANSACTION_CONTEXT_EVENT LogEventType = 36

	VIEW_CHANGE_EVENT LogEventType = 37

	/* Prepared XA transaction terminal event similar to Xid */
	XA_PREPARE_LOG_EVENT LogEventType = 38

	/**
	Extension of UPDATE_ROWS_EVENT, allowing partial values according
	to binlog_row_value_options.
	*/
	PARTIAL_UPDATE_ROWS_EVENT LogEventType = 39
)

const (
	BINLOG_EVENT_HEADER_LEN = 19
)

func (self LogEventType) String() string {
	switch self {
	case UNKNOWN_EVENT:
		return "UNKNOWN_EVENT"
	case START_EVENT_V3:
		return "START_EVENT_V3"
	case QUERY_EVENT:
		return "QUERY_EVENT"
	case STOP_EVENT:
		return "STOP_EVENT"
	case ROTATE_EVENT:
		return "ROTATE_EVENT"
	case INTVAR_EVENT:
		return "INTVAR_EVENT"
	case SLAVE_EVENT:
		return "SLAVE_EVENT"
	case APPEND_BLOCK_EVENT:
		return "APPEND_BLOCK_EVENT"
	case DELETE_FILE_EVENT:
		return "DELETE_FILE_EVENT"
	case RAND_EVENT:
		return "RAND_EVENT"
	case USER_VAR_EVENT:
		return "USER_VAR_EVENT"
	case FORMAT_DESCRIPTION_EVENT:
		return "FORMAT_DESCRIPTION_EVENT"
	case XID_EVENT:
		return "XID_EVENT"
	case BEGIN_LOAD_QUERY_EVENT:
		return "BEGIN_LOAD_QUERY_EVENT"
	case EXECUTE_LOAD_QUERY_EVENT:
		return "EXECUTE_LOAD_QUERY_EVENT"
	case TABLE_MAP_EVENT:
		return "TABLE_MAP_EVENT"
	case WRITE_ROWS_EVENT_V1:
		return "WRITE_ROWS_EVENT_V1"
	case UPDATE_ROWS_EVENT_V1:
		return "UPDATE_ROWS_EVENT_V1"
	case DELETE_ROWS_EVENT_V1:
		return "DELETE_ROWS_EVENT_V1"
	case INCIDENT_EVENT:
		return "INCIDENT_EVENT"
	case HEARTBEAT_LOG_EVENT:
		return "HEARTBEAT_LOG_EVENT"
	case IGNORABLE_LOG_EVENT:
		return "IGNORABLE_LOG_EVENT"
	case ROWS_QUERY_LOG_EVENT:
		return "ROWS_QUERY_LOG_EVENT"
	case WRITE_ROWS_EVENT:
		return "WRITE_ROWS_EVENT"
	case UPDATE_ROWS_EVENT:
		return "UPDATE_ROWS_EVENT"
	case DELETE_ROWS_EVENT:
		return "DELETE_FILE_EVENT"
	case GTID_LOG_EVENT:
		return "GTID_LOG_EVENT"
	case ANONYMOUS_GTID_LOG_EVENT:
		return "ANONYMOUS_GTID_LOG_EVENT"
	case PREVIOUS_GTIDS_LOG_EVENT:
		return "PREVIOUS_GTIDS_LOG_EVENT"
	case TRANSACTION_CONTEXT_EVENT:
		return "TRANSACTION_CONTEXT_EVENT"
	case VIEW_CHANGE_EVENT:
		return "VIEW_CHANGE_EVENT"
	case XA_PREPARE_LOG_EVENT:
		return "XA_PREPARE_LOG_EVENT"
	case PARTIAL_UPDATE_ROWS_EVENT:
		return "PARTIAL_UPDATE_ROWS_EVENT"
	default:
		return "INVALID"
	}
}

type BinLogEventHeader struct {
	Timestamp uint32 //  seconds since unix epoch
	EventType LogEventType
	ServerId  uint32 // server-id of the originating mysql-server. Used to filter out events in circular replication.
	EventSize uint32 // size of the event (header, post-header, body)
	LogPos    uint32 // position of the next event
	Flags     uint16
}

func (header *BinLogEventHeader) String() string {
	return fmt.Sprintf("{ timestamp: %d, eventType: %v, serverId: %d, eventSize: %d, logPos: %d, flags: %d }",
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

type Parser struct {
	file *os.File
	text []byte
}

func (self *Parser) readEventHeader() (*BinLogEventHeader, error) {
	self.text = self.text[0:BINLOG_EVENT_HEADER_LEN]
	n, err := self.file.Read(self.text)
	if err != nil {
		return nil, err
	}

	if n != BINLOG_EVENT_HEADER_LEN {
		return nil, errors.New("Failed to read event header")
	}

	return NewBinLogEventHeader(self.text)
}

func (self *Parser) ReadEvent() (BinLogEvent, error) {
	header, err := self.readEventHeader()
	if err != nil {
		return nil, err
	}

	size := header.EventSize - BINLOG_EVENT_HEADER_LEN
	if size != 0 {
		if uint32(cap(self.text)) < size {
			self.text = make([]byte, size)
		} else {
			self.text = self.text[:size]
		}

		n, err := self.file.Read(self.text)
		if err != nil {
			return nil, err
		}

		if n != len(self.text) {
			return nil, errors.New("Failed to read event body")
		}
	}

	return NewBinLogEvent(header, self.text), nil
}

func (self *Parser) SkipEvent() error {
	header, err := self.readEventHeader()
	if err != nil {
		return err
	}

	size := header.EventSize - BINLOG_EVENT_HEADER_LEN
	if size != 0 {
		if _, err = self.file.Seek(int64(size), 1); err != nil {
			return err
		}
	}

	return nil
}

func NewParser(file *os.File) (*Parser, error) {
	text := make([]byte, 4, 1024)
	n, err := file.Read(text)
	if err != nil {
		return nil, err
	}

	if n != 4 {
		return nil, errors.New("Failed to read binlog file header")
	}

	if text[0] != 0xfe || text[1] != 'b' || text[2] != 'i' || text[3] != 'n' {
		return nil, errors.New("Invalid binlog file header")
	}

	parser := new(Parser)
	parser.file = file
	parser.text = text
	return parser, nil
}

func usage() {
	fmt.Printf("Usage: %s <binlog>\n", os.Args[0])
	os.Exit(2)
}

func main() {
	var args struct {
		Path  string `arg:"-p,required" help:"binlog path"`
		Start int    `arg:"-s" default:"0" help:"start event"`
		Count int    `arg:"-c" default:"-1" help:"show event count"`
	}

	arg.MustParse(&args)
	file, err := os.Open(args.Path)
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	defer file.Close()

	var parser *Parser
	parser, err = NewParser(file)
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	for i := 0; i < args.Start; i++ {
		if err = parser.SkipEvent(); err != nil {
			panic(err)
		}
	}

	for i := 0; args.Count < 0 || i < args.Count; i++ {
		event, err := parser.ReadEvent()
		if err != nil {
			if err == io.EOF {
				break
			}

			panic(err)
		}

		fmt.Printf("----------------------EVENT-------------------\n")
		fmt.Printf("header: %s\npostHeader: %s\npayload: %s\n",
			event.GetHeader(), event.GetPostHeader(), event.GetPayload())
	}
}
