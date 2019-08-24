// tsbs_load_prometheus loads a Prometheus TSDB instance with data from stdin.
//
package main

import (
	"bufio"
	"flag"
	"log"
	"time"

	"github.com/timescale/tsbs/load"
)

// Program option vars:
var (
	dbPath string
)

// Global vars
var (
	loader *load.BenchmarkRunner
)

// allows for testing
var fatal = log.Fatalf

// Parse args:
func init() {
	loader = load.GetBenchmarkRunner()

	flag.StringVar(&dbPath, "path", "", "Prometheus TSDB storage path.")

	flag.Parse()
}

type benchmark struct{}

func (b *benchmark) GetPointDecoder(br *bufio.Reader) load.PointDecoder {
	return &decoder{scanner: bufio.NewScanner(br)}
}

func (b *benchmark) GetBatchFactory() load.BatchFactory {
	return &factory{}
}

func (b *benchmark) GetPointIndexer(_ uint) load.PointIndexer {
	return &load.ConstantIndexer{}
}

func (b *benchmark) GetProcessor() load.Processor {
	return &processor{}
}

func (b *benchmark) GetDBCreator() load.DBCreator {
	return &dbCreator{}
}

func main() {
	loader.RunBenchmark(&benchmark{}, load.SingleQueue)

	time.Sleep(2 * time.Second) // wait for TSDB to finish compaction
	tsdbStorage.Close()
}
