//
// events.go
// Copyright (C) 2019 Jianlong Chen <jianlong99@gmail.com>
//

package binlog

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/hashicorp/go-version"
	"io"
	"strings"
	"time"
)

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
		fmt.Sprintf("timestamp: %d (%v)", header.Timestamp, time.Unix(int64(header.Timestamp), 0)),
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
	MySQLServerVersion    string // version of the MySQL Server that created the binlog(50 bytes)
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
	header      *BinLogEventHeader
	payload     *FormatDescriptionEventPayload
	ChecksumAlg BinlogChecksumAlg
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
	header *BinLogEventHeader, text []byte) (*FormatDescriptionEventPayload, BinlogChecksumAlg, error) {

	size := header.EventSize - BINLOG_EVENT_HEADER_LEN
	if size != uint32(len(text)) {
		panic("Invalid FormatDescriptionEventPayload len")
	}

	r := bytes.NewReader(text)
	payload := new(FormatDescriptionEventPayload)
	err := binary.Read(r, binary.LittleEndian, payload.BinlogVersion)
	if err != nil {
		return nil, BINLOG_CHECKSUM_ALG_OFF, err
	}

	sversion := make([]byte, 50)
	if err = binary.Read(r, binary.LittleEndian, sversion); err != nil {
		return nil, BINLOG_CHECKSUM_ALG_OFF, err
	}

	for i := 0; i < 50; i++ {
		if sversion[i] == 0 {
			payload.MySQLServerVersion = string(sversion[:i])
			break
		}
	}

	if len(payload.MySQLServerVersion) == 0 {
		payload.MySQLServerVersion = string(sversion)
	}

	if err = binary.Read(r, binary.LittleEndian, (*payload).CreateTimestamp); err != nil {
		return nil, BINLOG_CHECKSUM_ALG_OFF, err
	}

	if err = binary.Read(r, binary.LittleEndian, payload.EventHeaderLength); err != nil {
		return nil, BINLOG_CHECKSUM_ALG_OFF, err
	}

	alg := BINLOG_CHECKSUM_ALG_OFF
	if version.Must(version.NewVersion(payload.MySQLServerVersion)).GreaterThanOrEqual(
		version.Must(version.NewVersion("5.6.1"))) {

		alg = BinlogChecksumAlg(text[len(text)-BINLOG_CHECKSUM_LEN-BINLOG_CHECKSUM_ALG_LEN])
		if alg >= BINLOG_CHECKSUM_ALG_END {
			panic("Invalid checksum algorithm")
		}

		size -= (BINLOG_CHECKSUM_LEN + BINLOG_CHECKSUM_ALG_LEN)
	}

	payload.EventTypeHeaderLength = make([]byte, size-(2+50+4+1))
	if err = binary.Read(r, binary.LittleEndian, payload.EventTypeHeaderLength); err != nil {
		return nil, alg, err
	}

	return payload, alg, err
}

func newFormatDescriptionEvent(header *BinLogEventHeader,
	text []byte) (*FormatDescriptionEvent, error) {

	payload, alg, err := newFormatDescriptionEventPayload(header, text)
	if err != nil {
		return nil, err
	}

	return &FormatDescriptionEvent{header, payload, alg}, nil
}

type XidEvent struct {
	header *BinLogEventHeader
	xid    uint64
}

func (event *XidEvent) GetHeader() []string {
	return event.header.Desc()
}

func (event *XidEvent) GetPostHeader() []string {
	return nil
}

func (event *XidEvent) GetPayload() []string {
	return []string{
		fmt.Sprintf("xid: %d", event.xid),
	}
}

func newXidEventPayload(header *BinLogEventHeader, text []byte) (xid uint64, err error) {
	size := header.EventSize - BINLOG_EVENT_HEADER_LEN
	if size != uint32(len(text)) {
		panic("Invalid XidEventPayload len")
	}

	r := bytes.NewReader(text)
	err = binary.Read(r, binary.LittleEndian, &xid)
	return
}

type QueryEventPostHeader struct {
	SlaveProxyId     uint32
	ExecutionTime    uint32
	SchemaLength     uint8
	ErrorCode        uint16
	StatusVarsLength uint16
}

func newQueryEventPostHeader(text []byte) (*QueryEventPostHeader, error) {
	if len(text) != QUERY_EVENT_POST_HEADER_LEN {
		panic("Invalid QueryEventPostHeader len")
	}

	post := new(QueryEventPostHeader)
	r := bytes.NewReader(text)
	err := binary.Read(r, binary.LittleEndian, post)
	return post, err
}

// QUERY_EVENT Payload StatusVars key
type QStatusKey uint8

const (
	Q_FLAGS2_CODE               QStatusKey = 0x00
	Q_SQL_MODE_CODE             QStatusKey = 0x01
	Q_CATALOG                   QStatusKey = 0x02
	Q_AUTO_INCREMENT            QStatusKey = 0x03
	Q_CHARSET_CODE              QStatusKey = 0x04
	Q_TIME_ZONE_CODE            QStatusKey = 0x05
	Q_CATALOG_NZ_CODE           QStatusKey = 0x06
	Q_LC_TIME_NAMES_CODE        QStatusKey = 0x07
	Q_CHARSET_DATABASE_CODE     QStatusKey = 0x08
	Q_TABLE_MAP_FOR_UPDATE_CODE QStatusKey = 0x09
	Q_MASTER_DATA_WRITTEN_CODE  QStatusKey = 0x0a
	Q_INVOKERS                  QStatusKey = 0x0b
	Q_UPDATED_DB_NAMES          QStatusKey = 0x0c
	Q_MICROSECONDS              QStatusKey = 0x0d
)

func (self QStatusKey) String() string {
	switch self {
	case Q_FLAGS2_CODE:
		return "Q_FLAGS2_CODE"
	case Q_SQL_MODE_CODE:
		return "Q_SQL_MODE_CODE"
	case Q_CATALOG:
		return "Q_CATALOG"
	case Q_AUTO_INCREMENT:
		return "Q_AUTO_INCREMENT"
	case Q_CHARSET_CODE:
		return "Q_CHARSET_CODE"
	case Q_TIME_ZONE_CODE:
		return "Q_TIME_ZONE_CODE"
	case Q_CATALOG_NZ_CODE:
		return "Q_CATALOG_NZ_CODE"
	case Q_LC_TIME_NAMES_CODE:
		return "Q_LC_TIME_NAMES_CODE"
	case Q_CHARSET_DATABASE_CODE:
		return "Q_CHARSET_DATABASE_CODE"
	case Q_TABLE_MAP_FOR_UPDATE_CODE:
		return "Q_TABLE_MAP_FOR_UPDATE_CODE"
	case Q_MASTER_DATA_WRITTEN_CODE:
		return "Q_MASTER_DATA_WRITTEN_CODE"
	case Q_INVOKERS:
		return "Q_INVOKERS"
	case Q_UPDATED_DB_NAMES:
		return "Q_UPDATED_DB_NAMES"
	case Q_MICROSECONDS:
		return "Q_MICROSECONDS"
	default:
		return "UNKNOWN"
	}
}

type QFlags2CodeType uint32

const (
	OPTION_AUTO_IS_NULL          QFlags2CodeType = 0x00004000
	OPTION_NOT_AUTOCOMMIT        QFlags2CodeType = 0x00080000
	OPTION_NO_FOREIGN_KEY_CHECKS QFlags2CodeType = 0x04000000
	OPTION_RELAXED_UNIQUE_CHECKS QFlags2CodeType = 0x08000000
)

func (self QFlags2CodeType) String() string {
	var val []string
	if self&OPTION_AUTO_IS_NULL != 0 {
		val = append(val, "OPTION_AUTO_IS_NULL")
	}

	if self&OPTION_NOT_AUTOCOMMIT != 0 {
		val = append(val, "OPTION_NOT_AUTOCOMMIT")
	}

	if self&OPTION_NO_FOREIGN_KEY_CHECKS != 0 {
		val = append(val, "OPTION_NO_FOREIGN_KEY_CHECKS")
	}

	if self&OPTION_RELAXED_UNIQUE_CHECKS != 0 {
		val = append(val, "OPTION_RELAXED_UNIQUE_CHECKS")
	}

	if len(val) == 0 {
		val = append(val, "(none)")
	}

	return strings.Join(val, " | ")
}

type QSQLModeCodeType uint64

const (
	MODE_REAL_AS_FLOAT              QSQLModeCodeType = 0x00000001
	MODE_PIPES_AS_CONCAT            QSQLModeCodeType = 0x00000002
	MODE_ANSI_QUOTES                QSQLModeCodeType = 0x00000004
	MODE_IGNORE_SPACE               QSQLModeCodeType = 0x00000008
	MODE_NOT_USED                   QSQLModeCodeType = 0x00000010
	MODE_ONLY_FULL_GROUP_BY         QSQLModeCodeType = 0x00000020
	MODE_NO_UNSIGNED_SUBTRACTION    QSQLModeCodeType = 0x00000040
	MODE_NO_DIR_IN_CREATE           QSQLModeCodeType = 0x00000080
	MODE_POSTGRESQL                 QSQLModeCodeType = 0x00000100
	MODE_ORACLE                     QSQLModeCodeType = 0x00000200
	MODE_MSSQL                      QSQLModeCodeType = 0x00000400
	MODE_DB2                        QSQLModeCodeType = 0x00000800
	MODE_MAXDB                      QSQLModeCodeType = 0x00001000
	MODE_NO_KEY_OPTIONS             QSQLModeCodeType = 0x00002000
	MODE_NO_TABLE_OPTIONS           QSQLModeCodeType = 0x00004000
	MODE_NO_FIELD_OPTIONS           QSQLModeCodeType = 0x00008000
	MODE_MYSQL323                   QSQLModeCodeType = 0x00010000
	MODE_MYSQL40                    QSQLModeCodeType = 0x00020000
	MODE_ANSI                       QSQLModeCodeType = 0x00040000
	MODE_NO_AUTO_VALUE_ON_ZERO      QSQLModeCodeType = 0x00080000
	MODE_NO_BACKSLASH_ESCAPES       QSQLModeCodeType = 0x00100000
	MODE_STRICT_TRANS_TABLES        QSQLModeCodeType = 0x00200000
	MODE_STRICT_ALL_TABLES          QSQLModeCodeType = 0x00400000
	MODE_NO_ZERO_IN_DATE            QSQLModeCodeType = 0x00800000
	MODE_NO_ZERO_DATE               QSQLModeCodeType = 0x01000000
	MODE_INVALID_DATES              QSQLModeCodeType = 0x02000000
	MODE_ERROR_FOR_DIVISION_BY_ZERO QSQLModeCodeType = 0x04000000
	MODE_TRADITIONAL                QSQLModeCodeType = 0x08000000
	MODE_NO_AUTO_CREATE_USER        QSQLModeCodeType = 0x10000000
	MODE_HIGH_NOT_PRECEDENCE        QSQLModeCodeType = 0x20000000
	MODE_NO_ENGINE_SUBSTITUTION     QSQLModeCodeType = 0x40000000
	MODE_PAD_CHAR_TO_FULL_LENGTH    QSQLModeCodeType = 0x80000000
)

func (self QSQLModeCodeType) String() string {
	var val []string
	if self&MODE_REAL_AS_FLOAT != 0 {
		val = append(val, "MODE_REAL_AS_FLOAT")
	}

	if self&MODE_PIPES_AS_CONCAT != 0 {
		val = append(val, "MODE_PIPES_AS_CONCAT")
	}

	if self&MODE_ANSI_QUOTES != 0 {
		val = append(val, "MODE_ANSI_QUOTES")
	}

	if self&MODE_IGNORE_SPACE != 0 {
		val = append(val, "MODE_IGNORE_SPACE")
	}

	if self&MODE_NOT_USED != 0 {
		val = append(val, "MODE_NOT_USED")
	}

	if self&MODE_ONLY_FULL_GROUP_BY != 0 {
		val = append(val, "MODE_ONLY_FULL_GROUP_BY")
	}

	if self&MODE_NO_UNSIGNED_SUBTRACTION != 0 {
		val = append(val, "MODE_NO_UNSIGNED_SUBTRACTION")
	}

	if self&MODE_NO_DIR_IN_CREATE != 0 {
		val = append(val, "MODE_NO_DIR_IN_CREATE")
	}

	if self&MODE_POSTGRESQL != 0 {
		val = append(val, "MODE_POSTGRESQL")
	}

	if self&MODE_ORACLE != 0 {
		val = append(val, "MODE_ORACLE")
	}

	if self&MODE_MSSQL != 0 {
		val = append(val, "MODE_MSSQL")
	}

	if self&MODE_DB2 != 0 {
		val = append(val, "MODE_DB2")
	}

	if self&MODE_MAXDB != 0 {
		val = append(val, "MODE_MAXDB")
	}

	if self&MODE_NO_KEY_OPTIONS != 0 {
		val = append(val, "MODE_NO_KEY_OPTIONS")
	}

	if self&MODE_NO_TABLE_OPTIONS != 0 {
		val = append(val, "MODE_NO_TABLE_OPTIONS")
	}

	if self&MODE_NO_FIELD_OPTIONS != 0 {
		val = append(val, "MODE_NO_FIELD_OPTIONS")
	}

	if self&MODE_MYSQL323 != 0 {
		val = append(val, "MODE_MYSQL323")
	}

	if self&MODE_MYSQL40 != 0 {
		val = append(val, "MODE_MYSQL40")
	}

	if self&MODE_ANSI != 0 {
		val = append(val, "MODE_ANSI")
	}

	if self&MODE_NO_AUTO_VALUE_ON_ZERO != 0 {
		val = append(val, "MODE_NO_AUTO_VALUE_ON_ZERO")
	}

	if self&MODE_NO_BACKSLASH_ESCAPES != 0 {
		val = append(val, "MODE_NO_BACKSLASH_ESCAPES")
	}

	if self&MODE_STRICT_TRANS_TABLES != 0 {
		val = append(val, "MODE_STRICT_TRANS_TABLES")
	}

	if self&MODE_STRICT_ALL_TABLES != 0 {
		val = append(val, "MODE_STRICT_ALL_TABLES")
	}

	if self&MODE_NO_ZERO_IN_DATE != 0 {
		val = append(val, "MODE_NO_ZERO_IN_DATE")
	}

	if self&MODE_NO_ZERO_DATE != 0 {
		val = append(val, "MODE_NO_ZERO_DATE")
	}

	if self&MODE_INVALID_DATES != 0 {
		val = append(val, "MODE_INVALID_DATES")
	}

	if self&MODE_ERROR_FOR_DIVISION_BY_ZERO != 0 {
		val = append(val, "MODE_ERROR_FOR_DIVISION_BY_ZERO")
	}

	if self&MODE_TRADITIONAL != 0 {
		val = append(val, "MODE_TRADITIONAL")
	}

	if self&MODE_NO_AUTO_CREATE_USER != 0 {
		val = append(val, "MODE_NO_AUTO_CREATE_USER")
	}

	if self&MODE_HIGH_NOT_PRECEDENCE != 0 {
		val = append(val, "MODE_HIGH_NOT_PRECEDENCE")
	}

	if self&MODE_NO_ENGINE_SUBSTITUTION != 0 {
		val = append(val, "MODE_NO_ENGINE_SUBSTITUTION")
	}

	if self&MODE_PAD_CHAR_TO_FULL_LENGTH != 0 {
		val = append(val, "MODE_PAD_CHAR_TO_FULL_LENGTH")
	}

	if len(val) == 0 {
		val = append(val, "(none)")
	}

	return strings.Join(val, " | ")
}

type QueryEventPayload struct {
	StatusVars map[QStatusKey]Any
	Schema     []byte
	Query      []byte
}

func newQueryEventPayload(header *BinLogEventHeader,
	postHeader *QueryEventPostHeader, text []byte) (payload *QueryEventPayload, err error) {

	payload = new(QueryEventPayload)
	payload.StatusVars = make(map[QStatusKey]Any)
	r := bytes.NewReader(text)
	var key QStatusKey
	for n := 0; n < int(postHeader.StatusVarsLength); {
		if err = binary.Read(r, binary.LittleEndian, &key); err != nil {
			return
		}

		n += 1
		switch key {
		case Q_FLAGS2_CODE, Q_MASTER_DATA_WRITTEN_CODE:
			var val uint32
			if err = binary.Read(r, binary.LittleEndian, &val); err != nil {
				return
			}

			if key == Q_FLAGS2_CODE {
				payload.StatusVars[key] = QFlags2CodeType(val)
			} else {
				payload.StatusVars[key] = val
			}
			n += 4
		case Q_SQL_MODE_CODE, Q_TABLE_MAP_FOR_UPDATE_CODE:
			var val uint64
			if err = binary.Read(r, binary.LittleEndian, &val); err != nil {
				return
			}

			if key == Q_SQL_MODE_CODE {
				payload.StatusVars[key] = QSQLModeCodeType(val)
			} else {
				payload.StatusVars[key] = val
			}
			n += 8
		case Q_CATALOG:
			var length uint8
			if err = binary.Read(r, binary.LittleEndian, &length); err != nil {
				return
			}

			val := make([]byte, length+1)
			if err = binary.Read(r, binary.LittleEndian, &val); err != nil {
				return
			}
			payload.StatusVars[key] = val
			n += (1 + int(length) + 1)
		case Q_AUTO_INCREMENT:
			val := make([]uint16, 2)
			if err = binary.Read(r, binary.LittleEndian, &val); err != nil {
				return
			}
			payload.StatusVars[key] = val
			n += (2 + 2)
		case Q_CHARSET_CODE:
			val := make([]uint16, 3)
			if err = binary.Read(r, binary.LittleEndian, &val); err != nil {
				return
			}
			payload.StatusVars[key] = val
			n += (2 + 2 + 2)
		case Q_TIME_ZONE_CODE, Q_CATALOG_NZ_CODE:
			var length uint8
			if err = binary.Read(r, binary.LittleEndian, &length); err != nil {
				return
			}

			val := make([]byte, length)
			if err = binary.Read(r, binary.LittleEndian, &val); err != nil {
				return
			}
			payload.StatusVars[key] = val
			n += (1 + int(length))
		case Q_LC_TIME_NAMES_CODE, Q_CHARSET_DATABASE_CODE:
			var val uint16
			if err = binary.Read(r, binary.LittleEndian, &val); err != nil {
				return
			}

			payload.StatusVars[key] = val
			n += 2
		case Q_UPDATED_DB_NAMES:
			var count uint8
			if err = binary.Read(r, binary.LittleEndian, &count); err != nil {
				return
			}

			val := make([]string, count)
			buf := make([]byte, postHeader.StatusVarsLength)
			for i := uint8(0); i < count; i++ {
				for j := 0; true; j++ {
					if err = binary.Read(r, binary.LittleEndian, &buf[j]); err != nil {
						return
					}

					if buf[j] == 0 {
						val[i] = string(buf[:j])
						n += (j + 1)
						break
					}
				}
			}

			payload.StatusVars[key] = val
			n += 1
		case Q_INVOKERS:
			var length uint8
			if err = binary.Read(r, binary.LittleEndian, &length); err != nil {
				return
			}

			username := make([]byte, length)
			if err = binary.Read(r, binary.LittleEndian, &username); err != nil {
				return
			}

			if err = binary.Read(r, binary.LittleEndian, &length); err != nil {
				return
			}

			hostname := make([]byte, length)
			if err = binary.Read(r, binary.LittleEndian, &hostname); err != nil {
				return
			}

			payload.StatusVars[key] = [][]byte{username, hostname}
			n += (1 + len(username) + 1 + len(hostname))
		case Q_MICROSECONDS:
			val := make([]byte, 3)
			if err = binary.Read(r, binary.LittleEndian, &val); err != nil {
				return
			}

			payload.StatusVars[key] = val
			n += 3
		}
	}

	payload.Schema = make([]byte, postHeader.SchemaLength)
	if err = binary.Read(r, binary.LittleEndian, &payload.Schema); err != nil {
		return
	}

	var dump uint8
	if err = binary.Read(r, binary.LittleEndian, &dump); err != nil {
		return
	}

	querySize := len(text) - int(postHeader.StatusVarsLength) - int(postHeader.SchemaLength) - 1
	payload.Query = make([]byte, querySize)
	err = binary.Read(r, binary.LittleEndian, &payload.Query)
	return
}

type QueryEvent struct {
	header     *BinLogEventHeader
	postHeader *QueryEventPostHeader
	payload    *QueryEventPayload
}

func (self *QueryEvent) GetHeader() []string {
	return self.header.Desc()
}

func (self *QueryEvent) GetPostHeader() []string {
	return []string{
		fmt.Sprintf("slave_proxy_id: %d", self.postHeader.SlaveProxyId),
		fmt.Sprintf("execution_time: %d", self.postHeader.ExecutionTime),
		fmt.Sprintf("schema_length: %d", self.postHeader.SchemaLength),
		fmt.Sprintf("error_code: %d", self.postHeader.ErrorCode),
		fmt.Sprintf("status_vars_length: %d", self.postHeader.StatusVarsLength),
	}
}

func (self *QueryEvent) GetPayload() []string {
	ret := []string{"status_vars:"}
	for key, val := range self.payload.StatusVars {
		ret = append(ret, fmt.Sprintf("\t%v: %v", key, val))
	}

	ret = append(ret, fmt.Sprintf("schema:\n%s", hex.Dump(self.payload.Schema)))
	ret = append(ret, fmt.Sprintf("query:\n%s", hex.Dump(self.payload.Query)))
	return ret
}

func newQueryEvent(header *BinLogEventHeader,
	text []byte, fde *FormatDescriptionEvent) (*QueryEvent, error) {

	size := header.EventSize - BINLOG_EVENT_HEADER_LEN
	if size != uint32(len(text)) {
		panic("Invalid QueryEvent")
	}

	postHeader, err := newQueryEventPostHeader(text[:QUERY_EVENT_POST_HEADER_LEN])
	if err != nil {
		return nil, err
	}

	end := len(text)
	if fde.ChecksumAlg == BINLOG_CHECKSUM_ALG_CRC32 {
		end -= BINLOG_CHECKSUM_LEN
	}

	fmt.Printf("eventsize=%d, size=%d, end=%d\n", header.EventSize, size, end)
	payload, err := newQueryEventPayload(header, postHeader, text[QUERY_EVENT_POST_HEADER_LEN:end])
	if err != nil {
		return nil, err
	}

	return &QueryEvent{header, postHeader, payload}, nil
}

type PreviousGtidsLogEvent struct {
	header   *BinLogEventHeader
	gtidSets []GTIDSet
}

func (self *PreviousGtidsLogEvent) GetHeader() []string {
	return self.header.Desc()
}

func (self *PreviousGtidsLogEvent) GetPostHeader() []string {
	return nil
}

func (self *PreviousGtidsLogEvent) GetPayload() []string {
	var val []string
	val = append(val, "gtid_sets:")
	for _, gtidset := range self.gtidSets {
		val = append(val, fmt.Sprintf("	%v:%d-%d, interval=%d",
			gtidset.Gtid, gtidset.From, gtidset.To, gtidset.Interval))
	}

	return val
}

func newPreviousGtidsLogEvent(header *BinLogEventHeader, text []byte,
	fde *FormatDescriptionEvent) (*PreviousGtidsLogEvent, error) {

	size := len(text)
	if fde.ChecksumAlg == BINLOG_CHECKSUM_ALG_CRC32 {
		size -= BINLOG_CHECKSUM_LEN
	}

	var count uint64
	r := bytes.NewReader(text)
	if err := binary.Read(r, binary.LittleEndian, &count); err != nil {
		return nil, err
	}

	event := new(PreviousGtidsLogEvent)
	event.header = header
	event.gtidSets = make([]GTIDSet, count)
	if err := binary.Read(r, binary.LittleEndian, &event.gtidSets); err != nil {
		return nil, err
	}

	return event, nil
}

func NewBinLogEvent(header *BinLogEventHeader,
	text []byte, fde *FormatDescriptionEvent) (BinLogEvent, error) {

	switch header.EventType {
	case FORMAT_DESCRIPTION_EVENT:
		ev, err := newFormatDescriptionEvent(header, text)
		if err != nil {
			return nil, err
		}

		fde.ChecksumAlg = ev.ChecksumAlg
		return ev, nil
	case XID_EVENT:
		xid, err := newXidEventPayload(header, text)
		if err != nil {
			return nil, err
		}

		return &XidEvent{header, xid}, nil
	case QUERY_EVENT:
		return newQueryEvent(header, text, fde)
	case PREVIOUS_GTIDS_LOG_EVENT:
		return newPreviousGtidsLogEvent(header, text, fde)
	default:
		return &UnknownBinLogEvent{header}, nil
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
