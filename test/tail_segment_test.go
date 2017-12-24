package test

import (
	"testing"
	"github.com/stretchr/testify/require"
	"os"
	"github.com/esdb/lstore"
	"context"
	"github.com/esdb/lstore/write"
	"github.com/esdb/lstore/search"
	"path"
)

func testStore() *lstore.Store {
	store := &lstore.Store{}
	store.Directory = "/tmp"
	os.Remove(path.Join(store.Directory, lstore.TailSegmentFileName))
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

func Test_write_read_one_entry(t *testing.T) {
	should := require.New(t)
	store := testStore()
	defer store.Stop(context.Background())
	offset, err := write.Execute(context.Background(), store, intEntry(1))
	should.Nil(err)
	should.Equal(lstore.Offset(0), offset)
	iter := search.Execute(context.Background(), store, 0, 2)
	defer iter.Close()
	rows, err := iter.Next()
	should.Nil(err)
	should.Equal(1, len(rows))
	should.Equal([]int64{1}, rows[0].IntValues)
}

func Test_write_two_entries(t *testing.T) {
	should := require.New(t)
	store := testStore()
	defer store.Stop(context.Background())
	offset, err := write.Execute(context.Background(), store, intEntry(1))
	should.Nil(err)
	should.Equal(lstore.Offset(0), offset)
	offset, err = write.Execute(context.Background(), store, intEntry(2))
	should.Nil(err)
	should.Equal(lstore.Offset(0x58), offset)
	iter := search.Execute(context.Background(), store, offset, 2)
	defer iter.Close()
	rows, err := iter.Next()
	should.Nil(err)
	should.Equal(1, len(rows))
	should.Equal([]int64{2}, rows[0].IntValues)
}

func Test_reopen(t *testing.T) {
	should := require.New(t)
	store := testStore()
	offset, err := write.Execute(context.Background(), store, intEntry(1))
	should.Nil(err)
	should.Equal(lstore.Offset(0), offset)
	store.Stop(context.Background())
	store = &lstore.Store{}
	store.Directory = "/tmp"
	err = store.Start()
	should.Nil(err)
	iter := search.Execute(context.Background(), store, 0, 2)
	defer iter.Close()
	rows, err := iter.Next()
	should.Nil(err)
	should.Equal(1, len(rows))
	should.Equal([]int64{1}, rows[0].IntValues)
	store.Stop(context.Background())
}

func Test_write_rotation(t *testing.T) {
	should := require.New(t)
	store := &lstore.Store{}
	store.Directory = "/tmp"
	store.TailSegmentMaxSize = 0x58
	os.Remove(path.Join(store.Directory, lstore.TailSegmentFileName))
	err := store.Start()
	should.Nil(err)
	offset, err := write.Execute(context.Background(), store, intEntry(1))
	should.Nil(err)
	should.Equal(lstore.Offset(0), offset)
	offset, err = write.Execute(context.Background(), store, intEntry(2))
	should.Nil(err)
	should.Equal(lstore.Offset(0x58), offset)
}
