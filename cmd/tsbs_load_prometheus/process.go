package main

import (
	"fmt"

	"github.com/timescale/tsbs/load"
)

// allows for testing
var printFn = fmt.Printf

type processor struct {
	dbWriter       *DBWriter
}

func (p *processor) Init(numWorker int, _ bool) {
	cfg := DBWriterConfig{
	}
	w := NewDBWriter(cfg)
	p.initWithDBWriter(numWorker, w)
}

func (p *processor) initWithDBWriter(numWorker int, w *DBWriter) {
	p.dbWriter = w
}

func (p *processor) Close(_ bool) {
}

func (p *processor) ProcessBatch(b load.Batch, doLoad bool) (uint64, uint64) {
	batch := b.(*batch)

	// Write the batch: try until backoff is not needed.
	if doLoad {
		_, err := p.dbWriter.Write(batch.points)
		if err != nil {
			fatal("Error writing: %s\n", err.Error())
		}
	}
	metricCnt := batch.metrics
	rowCnt := batch.rows

	return metricCnt, rowCnt
}
