//
// binlog.go
// Copyright (C) 2019 Jianlong Chen <jianlong99@gmail.com>
//
// MySQL binlog parser
//

package main

import (
	"github.com/alexflint/go-arg"
	. "github.com/chenjianlong/mysql-toolset/binlog"
	"io"
	"os"
)

func main() {
	var args struct {
		Path  string `arg:"-p,required" help:"binlog path"`
		Start int    `arg:"-s" default:"0" help:"start event"`
		Count int    `arg:"-c" default:"-1" help:"show event count"`
	}

	arg.MustParse(&args)
	file, err := os.Open(args.Path)
	if err != nil {
		panic(err)
	}

	defer file.Close()

	parser, err := NewParser(file)
	if err != nil {
		panic(err)
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

		PrintEvent(os.Stdout, event)
	}
}
