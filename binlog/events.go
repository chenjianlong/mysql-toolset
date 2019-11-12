//
// events.go
// Copyright (C) 2019 Jianlong Chen <jianlong99@gmail.com>
//

package binlog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
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

func (header *BinLogEventHeader) Desc() []string {
	return []string{
		fmt.Sprintf("timestamp: %d", header.Timestamp),
		fmt.Sprintf("event_type: %v", header.EventType),
		fmt.Sprintf("server_id: %d", header.ServerId),
		fmt.Sprintf("event_size: %d", header.EventSize),
		fmt.Sprintf("log_pos: %d", header.LogPos),
		fmt.Sprintf("flags: %d", header.Flags),
	}
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
	GetHeader() []string
	GetPostHeader() []string
	GetPayload() []string
}

type UnknownBinLogEvent struct {
	header *BinLogEventHeader
}

func (event *UnknownBinLogEvent) GetHeader() []string {
	return event.header.Desc()
}

func (event *UnknownBinLogEvent) GetPostHeader() []string {
	return nil
}

func (event *UnknownBinLogEvent) GetPayload() []string {
	return nil
}

type FormatDescriptionEventPayload struct {
	BinlogVersion         uint16 // version of this binlog format
	MySQLServerVersion    []byte // version of the MySQL Server that created the binlog
	CreateTimestamp       uint32 // seconds since Unix epoch when the binlog was created
	EventHeaderLength     uint8  // length of the Binlog Event Header of next events. Should always be 19
	EventTypeHeaderLength []byte // a array indexed by Binlog Event Type - 1 to extract the length of the event specific header
}

func (payload *FormatDescriptionEventPayload) Desc() []string {
	return []string{
		fmt.Sprintf("binlog_version: %d", payload.BinlogVersion),
		fmt.Sprintf("mysql_server_version: %s", payload.MySQLServerVersion),
		fmt.Sprintf("create_timestamp: %d", payload.CreateTimestamp),
		fmt.Sprintf("event_header_length: %d", payload.EventHeaderLength),
		fmt.Sprintf("event_type_header_length: %v", payload.EventTypeHeaderLength),
	}
}

type FormatDescriptionEvent struct {
	header  *BinLogEventHeader
	payload *FormatDescriptionEventPayload
}

func (event *FormatDescriptionEvent) GetHeader() []string {
	return event.header.Desc()
}

func (event *FormatDescriptionEvent) GetPostHeader() []string {
	return nil
}

func (event *FormatDescriptionEvent) GetPayload() []string {
	return event.payload.Desc()
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

func PrintEvent(w io.Writer, e BinLogEvent) {
	fmt.Fprintf(w, "----------------------EVENT-------------------\n")
	if header := e.GetHeader(); header != nil {
		fmt.Fprintf(w, "HEADER\n")
		for _, val := range header {
			fmt.Fprintf(w, "	%s\n", val)
		}
	}

	if postHeader := e.GetPostHeader(); postHeader != nil {
		fmt.Fprintf(w, "POST_HEADER\n")
		for _, val := range postHeader {
			fmt.Fprintf(w, "	%s\n", val)
		}
	}

	if payload := e.GetPayload(); payload != nil {
		fmt.Fprintf(w, "PAYLOAD\n")
		for _, val := range payload {
			fmt.Fprintf(w, "	%s\n", val)
		}
	}
}
