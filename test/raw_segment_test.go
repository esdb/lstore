package test

import (
	"testing"
	"github.com/stretchr/testify/require"
	"github.com/esdb/lstore"
	"github.com/esdb/lstore/write"
	"context"
	"os"
	"github.com/esdb/lstore/search"
)

func Test_raw_segment(t *testing.T) {
	should := require.New(t)
	store := &lstore.Store{}
	store.Directory = "/tmp"
	store.TailSegmentMaxSize = 140
	os.Remove(store.TailSegmentPath())
	err := store.Start()
	should.Nil(err)
	offset, err := write.Execute(context.Background(), store, intEntry(1))
	should.Nil(err)
	should.Equal(lstore.Offset(0), offset)
	offset, err = write.Execute(context.Background(), store, intEntry(2))
	should.Nil(err)
	should.Equal(lstore.Offset(88), offset)
	reader, err := store.NewReader()
	should.Nil(err)
	defer reader.Close()
	iter := search.Execute(context.Background(), reader, search.Request{
		LimitSize: 1,
	})
	rows, err := iter()
	should.Nil(err)
	should.Equal(1, len(rows))
	should.Equal([]int64{1}, rows[0].IntValues)
}