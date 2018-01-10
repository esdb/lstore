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
	store := testStore(lstore.Config{})
	defer store.Stop(ctx)
	seq, err := store.Write(ctx, intBlobEntry(1, ""))
	should.Nil(err)
	should.Equal(lstore.Offset(0), seq)
	reader, err := store.NewReader(ctx)
	should.Nil(err)
	defer reader.Close()
	collector := &lstore.RowsCollector{}
	reader.SearchForward(ctx, 0, nil, collector)
	should.Equal(1, len(collector.Rows))
	should.Equal([]int64{1}, collector.Rows[0].IntValues)
}

func Test_write_two_entries(t *testing.T) {
	should := require.New(t)
	store := testStore(lstore.Config{})
	defer store.Stop(ctx)
	offset, err := store.Write(ctx, intBlobEntry(1, ""))
	should.Nil(err)
	should.Equal(lstore.Offset(0), offset)
	offset, err = store.Write(ctx, intBlobEntry(2, ""))
	should.Nil(err)
	should.Equal(lstore.Offset(1), offset)
	reader, err := store.NewReader(ctx)
	should.Nil(err)
	defer reader.Close()
	collector := &lstore.RowsCollector{LimitSize: 2}
	reader.SearchForward(ctx, offset, nil, collector)
	should.Equal(1, len(collector.Rows))
	should.Equal([]int64{2}, collector.Rows[0].IntValues)
}

func Test_reopen_tail_segment(t *testing.T) {
	should := require.New(t)
	store := testStore(lstore.Config{})
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
	collector := &lstore.RowsCollector{LimitSize: 2}
	reader.SearchForward(ctx, 0, nil, collector)
	should.Equal(1, len(collector.Rows))
	should.Equal([]int64{1}, collector.Rows[0].IntValues)

	seq, err = store.Write(ctx, intEntry(2))
	should.Nil(err)
	should.Equal(lstore.Offset(1), seq)

	// can not read new rows without refresh
	collector = &lstore.RowsCollector{LimitSize: 2}
	reader.SearchForward(ctx, 0, nil, collector)
	should.Equal(1, len(collector.Rows))
	should.Equal([]int64{1}, collector.Rows[0].IntValues)

	// refresh, should read new rows now
	hasNew := reader.Refresh(ctx)
	should.True(hasNew)
	collector = &lstore.RowsCollector{LimitSize: 2}
	reader.SearchForward(ctx, 0, nil, collector)
	should.Equal(2, len(collector.Rows))
	should.Equal([]int64{1}, collector.Rows[0].IntValues)
	should.Equal([]int64{2}, collector.Rows[1].IntValues)
}

func Test_rotate_raw_segment_file(t *testing.T) {
	should := require.New(t)
	store := testStore(lstore.Config{TailSegmentMaxSize: 140})
	defer store.Stop(ctx)
	reader, err := store.NewReader(ctx)
	should.NoError(err)
	collector := &lstore.OffsetsCollector{}
	reader.SearchForward(ctx, 0, nil, collector)
	should.Len(collector.Offsets, 0)
	seq, err := store.Write(ctx, intEntry(1))
	should.Nil(err)
	should.Equal(lstore.Offset(0), seq)
	seq, err = store.Write(ctx, intEntry(2))
	should.Nil(err)
	should.Equal(lstore.Offset(1), seq)
	seq, err = store.Write(ctx, intEntry(3))
	should.Nil(err)
	should.Equal(lstore.Offset(2), seq)
	should.True(reader.Refresh(ctx))
	collector = &lstore.OffsetsCollector{}
	reader.SearchForward(ctx, 0, nil, collector)
	should.Len(collector.Offsets, 3)
}

func Test_rotate_raw_chunk_child(t *testing.T) {
	should := require.New(t)
	store := testStore(lstore.Config{})
	defer store.Stop(ctx)
	reader, err := store.NewReader(ctx)
	should.NoError(err)
	collector := &lstore.OffsetsCollector{}
	reader.SearchForward(ctx, 0, nil, collector)
	for i := 0; i < 65; i++ {
		seq, err := store.Write(ctx, intEntry(1))
		should.Nil(err)
		should.Equal(lstore.Offset(i), seq)
	}
	should.True(reader.Refresh(ctx))
	collector = &lstore.OffsetsCollector{}
	reader.SearchForward(ctx, 0, nil, collector)
	should.Equal(65, len(collector.Offsets))
}

func Test_rotate_raw_chunk(t *testing.T) {
	should := require.New(t)
	store := testStore(lstore.Config{})
	defer store.Stop(ctx)
	reader, err := store.NewReader(ctx)
	should.NoError(err)
	collector := &lstore.OffsetsCollector{}
	reader.SearchForward(ctx, 0, nil, collector)
	for i := 0; i < 4097; i++ {
		seq, err := store.Write(ctx, intEntry(1))
		should.Nil(err)
		should.Equal(lstore.Offset(i), seq)
	}
	should.True(reader.Refresh(ctx))
	collector = &lstore.OffsetsCollector{}
	reader.SearchForward(ctx, 0, nil, collector)
	should.Equal(4097, len(collector.Offsets))
}
