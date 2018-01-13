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

func testStore(config lstore.Config) *lstore.Store {
	store := &lstore.Store{Config: config}
	store.Directory = "/tmp/store"
	os.RemoveAll(store.Directory)
	err := store.Start(context.Background())
	if err != nil {
		panic(err)
	}
	return store
}

func reopenTestStore(store *lstore.Store) *lstore.Store {
	store.Stop(context.Background())
	newStore := &lstore.Store{}
	newStore.Config = store.Config
	err := newStore.Start(context.Background())
	if err != nil {
		panic(err)
	}
	return newStore
}

func intEntry(values ...int64) *lstore.Entry {
	return &lstore.Entry{EntryType: lstore.EntryTypeData, IntValues: values}
}

func blobEntry(values ...lstore.Blob) *lstore.Entry {
	return &lstore.Entry{EntryType: lstore.EntryTypeData, BlobValues: values}
}

func intBlobEntry(intValue int64, blobValue lstore.Blob) *lstore.Entry {
	return &lstore.Entry{EntryType: lstore.EntryTypeData, IntValues: []int64{intValue}, BlobValues: []lstore.Blob{blobValue}}
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