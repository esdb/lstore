package test

import (
	"testing"
	"github.com/stretchr/testify/require"
	"github.com/esdb/lstore"
	"context"
)

func Test_write_read_one_entry(t *testing.T) {
	should := require.New(t)
	store := bigTestStore()
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
	store := bigTestStore()
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
		LimitSize:   2,
	})
	rows, err := iter()
	should.Nil(err)
	should.Equal(1, len(rows))
	should.Equal([]int64{2}, rows[0].IntValues)
}

func Test_reopen_tail_segment(t *testing.T) {
	should := require.New(t)
	store := bigTestStore()
	defer store.Stop(context.Background())
	offset, err := store.Write(context.Background(), intEntry(1))
	should.Nil(err)
	should.Equal(lstore.Offset(0), offset)

	store = reopenTestStore(store)

	// can read rows from disk
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

	offset, err = store.Write(context.Background(), intEntry(2))
	should.Nil(err)
	should.Equal(lstore.Offset(0x58), offset)

	// can not read new rows without refresh
	iter = reader.Search(context.Background(), lstore.SearchRequest{
		LimitSize: 2,
	})
	rows, err = iter()
	should.Nil(err)
	should.Equal(1, len(rows))
	should.Equal([]int64{1}, rows[0].IntValues)

	// refresh, should read new rows now
	reader.Refresh()
	iter = reader.Search(context.Background(), lstore.SearchRequest{
		LimitSize: 2,
	})
	rows, err = iter()
	should.Nil(err)
	should.Equal(2, len(rows))
	should.Equal([]int64{1}, rows[0].IntValues)
	should.Equal([]int64{2}, rows[1].IntValues)
}

func Test_write_rotation(t *testing.T) {
	should := require.New(t)
	store := tinyTestStore()
	defer store.Stop(context.Background())
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
