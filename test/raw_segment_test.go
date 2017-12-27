package test

import (
	"testing"
	"github.com/stretchr/testify/require"
	"github.com/esdb/lstore"
	"context"
	"io"
)

func Test_raw_segment(t *testing.T) {
	should := require.New(t)
	store := tinyTestStore()
	defer store.Stop(context.Background())
	seq, err := store.Write(context.Background(), intEntry(1))
	should.Nil(err)
	should.Equal(lstore.RowSeq(0), seq)
	seq, err = store.Write(context.Background(), intEntry(2))
	should.Nil(err)
	should.Equal(lstore.RowSeq(88), seq)
	reader, err := store.NewReader()
	should.Nil(err)
	defer reader.Close()
	iter := reader.Search(context.Background(), lstore.SearchRequest{
		LimitSize: 1,
	})
	rows, err := iter()
	should.Nil(err)
	should.Equal(1, len(rows))
	should.Equal([]int64{1}, rows[0].IntValues)
	rows, err = iter()
	should.Equal(io.EOF, err)
}

func Test_reopen_raw_segment(t *testing.T) {
	should := require.New(t)
	store := tinyTestStore()
	defer store.Stop(context.Background())
	seq, err := store.Write(context.Background(), intEntry(1))
	should.Nil(err)
	should.Equal(lstore.RowSeq(0), seq)
	seq, err = store.Write(context.Background(), intEntry(2))
	should.Nil(err)
	should.Equal(lstore.RowSeq(88), seq)

	store.Stop(context.Background())
	store = &lstore.Store{}
	store.Directory = "/tmp"
	err = store.Start()
	should.Nil(err)

	reader, err := store.NewReader()
	should.Nil(err)
	defer reader.Close()
	iter := reader.Search(context.Background(), lstore.SearchRequest{
		LimitSize: 1,
	})
	rows, err := iter()
	should.Nil(err)
	should.Equal(1, len(rows))
	should.Equal([]int64{1}, rows[0].IntValues)
}