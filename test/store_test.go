package test

import (
	"github.com/esdb/lstore"
	"context"
	"os"
	"github.com/v2pro/plz"
	"testing"
	"github.com/v2pro/plz/concurrent"
	"github.com/v2pro/plz/countlog"
)

func TestMain(m *testing.M) {
	defer concurrent.GlobalUnboundedExecutor.StopAndWaitForever()
	plz.LogLevel = countlog.LevelDebug
	plz.PlugAndPlay()
	m.Run()
}

func testStore(cfg *lstore.Config) *lstore.Store {
	cfg.Directory = "/run/store"
	os.RemoveAll(cfg.Directory)
	store, err := lstore.New(context.Background(), cfg)
	if err != nil {
		panic(err)
	}
	return store
}

func reopenTestStore(store *lstore.Store) *lstore.Store {
	store.Stop(context.Background())
	cfg := store.Config()
	newStore, err := lstore.New(context.Background(), &cfg)
	if err != nil {
		panic(err)
	}
	return newStore
}

func intEntry(values ...int64) *lstore.Entry {
	return &lstore.Entry{IntValues: values}
}

func blobEntry(values ...lstore.Blob) *lstore.Entry {
	return &lstore.Entry{BlobValues: values}
}

func intBlobEntry(intValue int64, blobValue lstore.Blob) *lstore.Entry {
	return &lstore.Entry{IntValues: []int64{intValue}, BlobValues: []lstore.Blob{blobValue}}
}

type assertSearchForward struct {
	cb lstore.SearchCallback
	lastOffset lstore.Offset
}

func (cb *assertSearchForward) HandleRow(offset lstore.Offset, entry *lstore.Entry) error {
	if offset < cb.lastOffset {
		panic("not forward")
	}
	cb.lastOffset = offset
	return cb.cb.HandleRow(offset, entry)
}

type assertContinuous struct {
	cb lstore.SearchCallback
	lastOffset lstore.Offset
}

func (cb *assertContinuous) HandleRow(offset lstore.Offset, entry *lstore.Entry) error {
	if offset > 0 && offset != cb.lastOffset + 1 {
		panic("not continuous")
	}
	cb.lastOffset = offset
	return cb.cb.HandleRow(offset, entry)
}