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
	return fmt.Sprintf("{ timestamp: %d, eventType: %d, serverId: %d, eventSize: %d, logPos: %d, flags: %d}", header.Timestamp, header.EventType, header.ServerId, header.EventSize, header.LogPos, header.Flags)
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

func usage() {
	fmt.Printf("Usage: %s <binlog>\n", os.Args[0])
	os.Exit(2)
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

	fmt.Println(header)
}
