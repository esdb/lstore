package test

import (
	"testing"
	"github.com/stretchr/testify/require"
	"github.com/esdb/lstore"
)

func Test_raw_segment(t *testing.T) {
	should := require.New(t)
	cfg := &lstore.Config{}
	cfg.RawSegmentMaxSizeInBytes = 140
	store := testStore(cfg)
	defer store.Stop(ctx)
	seq, err := store.Append(ctx, intEntry(1))
	should.Nil(err)
	should.Equal(lstore.Offset(1), seq)
	seq, err = store.Append(ctx, intEntry(2))
	should.Nil(err)
	should.Equal(lstore.Offset(2), seq)
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
	cfg := &lstore.Config{}
	cfg.RawSegmentMaxSizeInBytes = 140
	store := testStore(cfg)
	defer store.Stop(ctx)
	for i := 0; i < 256; i++ {
		seq, err := store.Append(ctx, intEntry(int64(i)))
		should.Nil(err)
		should.Equal(lstore.Offset(i+1), seq)
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
	cfg := &lstore.Config{}
	cfg.RawSegmentMaxSizeInBytes = 140
	store := testStore(cfg)
	seq, err := store.Append(ctx, intEntry(1))
	should.Nil(err)
	should.Equal(lstore.Offset(1), seq)
	seq, err = store.Append(ctx, intEntry(2))
	should.Nil(err)
	should.Equal(lstore.Offset(2), seq)

	store = reopenTestStore(store)
	defer store.Stop(ctx)

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
