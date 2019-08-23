package main

// This file lifted wholesale from mountainflux by Mark Rushakoff.

import (
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/labels"
)

type DBWriterConfig struct {
	DBPath string
}

// DBWriter is a Writer that writes to a Prometheus TSDB.
type DBWriter struct {
	storage *tsdb.DB
}

// NewDBWriter returns a new DBWriter from the supplied DBWriterConfig.
func NewDBWriter(c DBWriterConfig) *DBWriter {
	return &DBWriter{
		storage: tsdbStorage,
	}
}

func (w *DBWriter) Write(points []string) (int64, error) {
	start := time.Now()

	app := w.storage.Appender()

	for _, point := range points {
		var labs labels.Labels

		args := strings.Split(point, " ")

		tags := strings.Split(args[0], ",")
		for _, tag := range tags[1:] {
			ss := strings.Split(tag, "=")
			labs = append(labs, labels.Label{Name: ss[0], Value: ss[1]})
		}

		timestamp, _ := strconv.ParseInt(args[2], 10, 64)
		timestamp /= 1000000
		metrics := strings.Split(args[1], ",")
		for _, metric := range metrics {
			ss := strings.Split(metric, "=")
			metric_name := ss[0]
			metric_labs := make([]labels.Label, len(labs))
			copy(metric_labs, labs)
			metric_labs = append(metric_labs, labels.Label{Name: "__name__", Value: metric_name})

			value, _ := strconv.Atoi(ss[1][:len(ss[1])-1])

			app.Add(metric_labs, timestamp, float64(value))
		}
	}

	app.Commit()

	lat := time.Since(start).Nanoseconds()
	return lat, nil
}
