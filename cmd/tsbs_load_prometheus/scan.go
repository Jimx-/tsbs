package main

import (
	"bufio"
	"strings"

	"github.com/timescale/tsbs/load"
)

const errNotThreeTuplesFmt = "parse error: line does not have 3 tuples, has %d"

var newLine = []byte("\n")

type decoder struct {
	scanner *bufio.Scanner
}

func (d *decoder) Decode(_ *bufio.Reader) *load.Point {
	ok := d.scanner.Scan()
	if !ok && d.scanner.Err() == nil { // nothing scanned & no error = EOF
		return nil
	} else if !ok {
		fatal("scan error: %v", d.scanner.Err())
		return nil
	}
	return load.NewPoint(d.scanner.Bytes())
}

type batch struct {
	points  []string
	rows    uint64
	metrics uint64
}

func (b *batch) Len() int {
	return int(b.rows)
}

func (b *batch) Append(item *load.Point) {
	that := item.Data.([]byte)
	thatStr := string(that)

	b.rows++
	// Each influx line is format "csv-tags csv-fields timestamp", so we split by space
	// and then on the middle element, we split by comma to count number of fields added
	args := strings.Split(thatStr, " ")
	if len(args) != 3 {
		fatal(errNotThreeTuplesFmt, len(args))
		return
	}

	b.metrics += uint64(len(strings.Split(args[1], ",")))
	b.points = append(b.points, thatStr)
}

type factory struct{}

func (f *factory) New() load.Batch {
	return &batch{points: nil, rows: 0, metrics: 0}
}
