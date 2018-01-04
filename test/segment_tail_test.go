package test

import (
	"testing"
	"github.com/stretchr/testify/require"
	"github.com/esdb/lstore"
	"context"
)

var ctx = context.Background()

func Test_write_read_one_entry(t *testing.T) {
	should := require.New(t)
	store := bigTestStore()
	defer store.Stop(ctx)
	seq, err := store.Write(ctx, intEntry(1))
	should.Nil(err)
	should.Equal(lstore.Offset(0), seq)
	reader, err := store.NewReader(ctx)
	should.Nil(err)
	defer reader.Close()
	collector := &lstore.ResultCollector{}
	reader.SearchForward(ctx, 0, nil, collector)
	should.Equal(1, len(collector.Rows))
	should.Equal([]int64{1}, collector.Rows[0].IntValues)
}

func Test_write_two_entries(t *testing.T) {
	should := require.New(t)
	store := bigTestStore()
	defer store.Stop(ctx)
	seq, err := store.Write(ctx, intEntry(1))
	should.Nil(err)
	should.Equal(lstore.Offset(0), seq)
	seq, err = store.Write(ctx, intEntry(2))
	should.Nil(err)
	should.Equal(lstore.Offset(1), seq)
	reader, err := store.NewReader(ctx)
	should.Nil(err)
	defer reader.Close()
	collector := &lstore.ResultCollector{LimitSize: 2}
	reader.SearchForward(ctx, seq, nil, collector)
	should.Equal(1, len(collector.Rows))
	should.Equal([]int64{2}, collector.Rows[0].IntValues)
}

func Test_reopen_tail_segment(t *testing.T) {
	should := require.New(t)
	store := bigTestStore()
	defer store.Stop(ctx)
	seq, err := store.Write(ctx, intEntry(1))
	should.Nil(err)
	should.Equal(lstore.Offset(0), seq)

	store = reopenTestStore(store)

	// can read rows from disk
	reader, err := store.NewReader(ctx)
	should.Equal(lstore.Offset(1), reader.TailOffset())
	should.Nil(err)
	defer reader.Close()
	collector := &lstore.ResultCollector{LimitSize: 2}
	reader.SearchForward(ctx, 0, nil, collector)
	should.Equal(1, len(collector.Rows))
	should.Equal([]int64{1}, collector.Rows[0].IntValues)

	seq, err = store.Write(ctx, intEntry(2))
	should.Nil(err)
	should.Equal(lstore.Offset(1), seq)

	// can not read new rows without refresh
	collector = &lstore.ResultCollector{LimitSize: 2}
	reader.SearchForward(ctx, 0, nil, collector)
	should.Equal(1, len(collector.Rows))
	should.Equal([]int64{1}, collector.Rows[0].IntValues)

	// refresh, should read new rows now
	hasNew, err := reader.Refresh(ctx)
	should.Nil(err)
	should.True(hasNew)
	collector = &lstore.ResultCollector{LimitSize: 2}
	reader.SearchForward(ctx, 0, nil, collector)
	should.Equal(2, len(collector.Rows))
	should.Equal([]int64{1}, collector.Rows[0].IntValues)
	should.Equal([]int64{2}, collector.Rows[1].IntValues)
}

func Test_write_rotation(t *testing.T) {
	should := require.New(t)
	store := tinyTestStore()
	defer store.Stop(ctx)
	seq, err := store.Write(ctx, intEntry(1))
	should.Nil(err)
	should.Equal(lstore.Offset(0), seq)
	seq, err = store.Write(ctx, intEntry(2))
	should.Nil(err)
	should.Equal(lstore.Offset(1), seq)
	seq, err = store.Write(ctx, intEntry(3))
	should.Nil(err)
	should.Equal(lstore.Offset(2), seq)
}
