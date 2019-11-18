//
// consts.go
// Copyright (C) 2019 Jianlong Chen <jianlong99@gmail.com>
//

package binlog

import (
	"github.com/google/uuid"
)

type Any interface{}

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

type BinlogChecksumAlg uint8

const (
	BINLOG_CHECKSUM_ALG_OFF   BinlogChecksumAlg = 0
	BINLOG_CHECKSUM_ALG_CRC32 BinlogChecksumAlg = 1
	BINLOG_CHECKSUM_ALG_END   BinlogChecksumAlg = 2
)

const (
	BINLOG_EVENT_HEADER_LEN     = 19
	QUERY_EVENT_POST_HEADER_LEN = 13
	BINLOG_CHECKSUM_LEN         = 4
	BINLOG_CHECKSUM_ALG_LEN     = 1
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

type GTIDSet struct {
	Gtid     uuid.UUID
	Interval uint64
	From     uint64
	To       uint64
}
