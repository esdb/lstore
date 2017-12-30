package test

import (
	"github.com/esdb/lstore"
	"context"
	"os"
	"github.com/v2pro/plz/countlog"
)

func init() {
	countlog.Setup(countlog.Config{})
}

func bigTestStore() *lstore.Store {
	store := &lstore.Store{}
	store.Directory = "/tmp/store"
	store.TailSegmentMaxSize = 200 * 1024 * 1024
	os.RemoveAll(store.Directory)
	err := store.Start()
	if err != nil {
		panic(err)
	}
	return store
}

func tinyTestStore() *lstore.Store {
	store := &lstore.Store{}
	store.Directory = "/tmp/store"
	store.TailSegmentMaxSize = 140
	os.RemoveAll(store.Directory)
	err := store.Start()
	if err != nil {
		panic(err)
	}
	return store
}

func smallTestStore() *lstore.Store {
	store := &lstore.Store{}
	store.Directory = "/tmp/store"
	store.TailSegmentMaxSize = 280
	store.BloomFilterIndexedBlobColumns = []int{0}
	os.RemoveAll(store.Directory)
	err := store.Start()
	if err != nil {
		panic(err)
	}
	return store
}

func reopenTestStore(store *lstore.Store) *lstore.Store {
	store.Stop(context.Background())
	newStore := &lstore.Store{}
	newStore.Config = store.Config
	err := newStore.Start()
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
