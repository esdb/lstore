package test

import (
	"testing"
	"github.com/stretchr/testify/require"
	"github.com/esdb/lstore"
)

func Test_raw_segment(t *testing.T) {
	should := require.New(t)
	store := testStore(lstore.Config{TailSegmentMaxSize: 140})
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
	collector := &lstore.RowsCollector{LimitSize: 1}
	reader.SearchForward(ctx, &lstore.SearchRequest{
		0, nil, collector,
	})
	should.Equal(1, len(collector.Rows))
	should.Equal([]int64{1}, collector.Rows[0].IntValues)
}

func Test_a_lot_raw_segment(t *testing.T) {
	should := require.New(t)
	store := testStore(lstore.Config{TailSegmentMaxSize: 140})
	defer store.Stop(ctx)
	for i := 0; i < 256; i++ {
		seq, err := store.Write(ctx, intEntry(int64(i)))
		should.Nil(err)
		should.Equal(lstore.Offset(i), seq)
	}
	reader, err := store.NewReader(ctx)
	should.Nil(err)
	defer reader.Close()
	collector := &lstore.RowsCollector{}
	reader.SearchForward(ctx, &lstore.SearchRequest{
		0, nil, collector,
	})
	should.Equal(256, len(collector.Rows))
	should.Equal([]int64{0}, collector.Rows[0].IntValues)
}

func Test_reopen_raw_segment(t *testing.T) {
	should := require.New(t)
	store := testStore(lstore.Config{TailSegmentMaxSize: 140})
	defer store.Stop(ctx)
	seq, err := store.Write(ctx, intEntry(1))
	should.Nil(err)
	should.Equal(lstore.Offset(0), seq)
	seq, err = store.Write(ctx, intEntry(2))
	should.Nil(err)
	should.Equal(lstore.Offset(1), seq)

	store = reopenTestStore(store)

	reader, err := store.NewReader(ctx)
	should.Nil(err)
	defer reader.Close()
	collector := &lstore.RowsCollector{LimitSize: 1}
	reader.SearchForward(ctx, &lstore.SearchRequest{
		0, nil, collector,
	})
	should.Equal(1, len(collector.Rows))
	should.Equal([]int64{1}, collector.Rows[0].IntValues)
}
