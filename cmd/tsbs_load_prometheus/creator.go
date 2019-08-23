package main

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/prometheus/tsdb"
)

type dbCreator struct {
	dbPath  string
	logger  log.Logger
	storage *tsdb.DB
}

var (
	tsdbStorage *tsdb.DB
)

func (d *dbCreator) Init() {
}

func (d *dbCreator) DBExists(dbName string) bool {
	return true
}

func (d *dbCreator) RemoveOldDB(dbName string) error {
	return nil
}

func (d *dbCreator) CreateDB(dbName string) error {
	d.dbPath = dbPath

	if dbPath == "" {
		dir, err := ioutil.TempDir("", "tsdb_bench")
		if err != nil {
			return err
		}
		d.dbPath = dir
	}

	if err := os.RemoveAll(d.dbPath); err != nil {
		return err
	}

	if err := os.MkdirAll(d.dbPath, 0777); err != nil {
		return err
	}

	dir := filepath.Join(d.dbPath, "storage")

	d.logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	l := log.With(d.logger, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller)

	st, err := tsdb.Open(dir, l, nil, &tsdb.Options{
		RetentionDuration: 15 * 24 * 60 * 60 * 1000, // 15 days in milliseconds
		BlockRanges:       tsdb.ExponentialBlockRanges(2*60*60*1000, 5, 3),
	})

	if err != nil {
		return err
	}

	tsdbStorage = st
	d.storage = st

	return nil
}
