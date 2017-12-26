package test

import (
	"github.com/esdb/lstore"
	"path"
	"context"
	"os"
)

func bigTestStore() *lstore.Store {
	store := &lstore.Store{}
	store.Directory = "/tmp"
	store.TailSegmentMaxSize = 200 * 1024 * 1024
	os.Remove(path.Join(store.Directory, lstore.TailSegmentFileName))
	err := store.Start()
	if err != nil {
		panic(err)
	}
	return store
}

func tinyTestStore() *lstore.Store {
	store := &lstore.Store{}
	store.Directory = "/tmp"
	store.TailSegmentMaxSize = 140
	os.Remove(path.Join(store.Directory, lstore.TailSegmentFileName))
	err := store.Start()
	if err != nil {
		panic(err)
	}
	return store
}

func reopenTestStore(store *lstore.Store) *lstore.Store {
	store.Stop(context.Background())
	store = &lstore.Store{}
	store.Directory = "/tmp"
	err := store.Start()
	if err != nil {
		panic(err)
	}
	return store
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
