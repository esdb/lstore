package test

import (
	"testing"
	"github.com/stretchr/testify/require"
	"os"
	"github.com/esdb/lstore"
	"context"
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
	offset, err := store.Write(context.Background(), intEntry(1))
	should.Nil(err)
	should.Equal(lstore.Offset(0), offset)
	reader, err := store.NewReader()
	should.Nil(err)
	defer reader.Close()
	iter := reader.Search(context.Background(), lstore.SearchRequest{
		LimitSize: 2,
	})
	rows, err := iter()
	should.Nil(err)
	should.Equal(1, len(rows))
	should.Equal([]int64{1}, rows[0].IntValues)
}

func Test_write_two_entries(t *testing.T) {
	should := require.New(t)
	store := testStore()
	defer store.Stop(context.Background())
	offset, err := store.Write(context.Background(), intEntry(1))
	should.Nil(err)
	should.Equal(lstore.Offset(0), offset)
	offset, err = store.Write(context.Background(), intEntry(2))
	should.Nil(err)
	should.Equal(lstore.Offset(0x58), offset)
	reader, err := store.NewReader()
	should.Nil(err)
	defer reader.Close()
	iter := reader.Search(context.Background(), lstore.SearchRequest{
		StartOffset: offset,
		LimitSize: 2,
	})
	rows, err := iter()
	should.Nil(err)
	should.Equal(1, len(rows))
	should.Equal([]int64{2}, rows[0].IntValues)
}

func Test_reopen_tail_segment(t *testing.T) {
	should := require.New(t)
	store := testStore()
	offset, err := store.Write(context.Background(), intEntry(1))
	should.Nil(err)
	should.Equal(lstore.Offset(0), offset)

	store.Stop(context.Background())
	store = &lstore.Store{}
	store.Directory = "/tmp"
	err = store.Start()
	should.Nil(err)

	reader, err := store.NewReader()
	should.Nil(err)
	defer reader.Close()
	iter := reader.Search(context.Background(), lstore.SearchRequest{
		LimitSize: 2,
	})
	rows, err := iter()
	should.Nil(err)
	should.Equal(1, len(rows))
	should.Equal([]int64{1}, rows[0].IntValues)
	store.Stop(context.Background())
}

func Test_write_rotation(t *testing.T) {
	should := require.New(t)
	store := &lstore.Store{}
	defer store.Stop(context.Background())
	store.Directory = "/tmp"
	store.TailSegmentMaxSize = 140
	os.Remove(store.TailSegmentPath())
	err := store.Start()
	should.Nil(err)
	offset, err := store.Write(context.Background(), intEntry(1))
	should.Nil(err)
	should.Equal(lstore.Offset(0), offset)
	offset, err = store.Write(context.Background(), intEntry(2))
	should.Nil(err)
	should.Equal(lstore.Offset(88), offset)
	offset, err = store.Write(context.Background(), intEntry(3))
	should.Nil(err)
	should.Equal(lstore.Offset(176), offset)
}
