//
// parser.go
// Copyright (C) 2019 Jianlong Chen <jianlong99@gmail.com>
//

package binlog

import (
	"errors"
	"os"
)

type Parser struct {
	file *os.File
	text []byte
	fde  *FormatDescriptionEvent
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
	if self.fde == nil {
		self.fde = new(FormatDescriptionEvent)
	}

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

	return NewBinLogEvent(header, self.text, self.fde)
}

func (self *Parser) SkipEvent() error {
	if self.fde == nil {
		_, err := self.ReadEvent()
		return err
	}

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
	parser.fde = nil
	return parser, nil
}
