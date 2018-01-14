package test

import (
	"testing"
	"github.com/stretchr/testify/require"
	"github.com/esdb/lstore"
	"strconv"
)

func Test_indexing_segment(t *testing.T) {
	should := require.New(t)
	cfg := &lstore.Config{}
	cfg.RawSegmentMaxSizeInBytes = 280
	cfg.BloomFilterIndexedBlobColumns = []int{0}
	store := testStore(cfg)
	defer store.Stop(ctx)
	for i := 0; i < 260; i++ {
		blobValue := lstore.Blob("hello")
		if i%2 == 0 {
			blobValue = lstore.Blob("world")
		}
		_, err := store.Write(ctx, intBlobEntry(int64(i)+1, blobValue))
		should.Nil(err)
	}
	should.Nil(store.UpdateIndex(ctx))
	reader, err := store.NewReader(ctx)
	should.Nil(err)
	collector := &lstore.RowsCollector{}
	reader.SearchForward(ctx, &lstore.SearchRequest{
		0, store.NewBlobValueFilter(0, "hello"), collector,
	})
	should.Len(collector.Rows, 130)
	should.Equal([]int64{2}, collector.Rows[0].IntValues)
	should.Equal([]int64{4}, collector.Rows[1].IntValues)
}

func Test_reopen_indexing_segment(t *testing.T) {
	should := require.New(t)
	cfg := &lstore.Config{}
	cfg.BloomFilterIndexedBlobColumns = []int{0}
	store := testStore(cfg)
	for i := 0; i < 260; i++ {
		blobValue := lstore.Blob("hello")
		if i%2 == 0 {
			blobValue = lstore.Blob("world")
		}
		_, err := store.Write(ctx, intBlobEntry(int64(i)+1, blobValue))
		should.Nil(err)
	}
	should.Nil(store.UpdateIndex(ctx))

	store = reopenTestStore(store)
	defer store.Stop(ctx)

	reader, err := store.NewReader(ctx)
	should.Nil(err)
	collector := &lstore.RowsCollector{LimitSize: 2}
	reader.SearchForward(ctx, &lstore.SearchRequest{
		0, store.NewBlobValueFilter(0, "hello"), collector,
	})
	should.Equal([]int64{2}, collector.Rows[0].IntValues)
	should.Equal([]int64{4}, collector.Rows[1].IntValues)
}

func Test_index_twice_should_not_repeat_rows(t *testing.T) {
	should := require.New(t)
	cfg := &lstore.Config{}
	cfg.BloomFilterIndexedBlobColumns = []int{0}
	store := testStore(cfg)
	defer store.Stop(ctx)
	for i := 0; i < 260; i++ {
		blobValue := lstore.Blob(strconv.Itoa(i))
		offset, err := store.Write(ctx, intBlobEntry(int64(i), blobValue))
		should.Nil(err)
		should.Equal(lstore.Offset(i + 1), offset)
	}
	should.Nil(store.UpdateIndex(ctx))
	for i := 260; i < 520; i++ {
		blobValue := lstore.Blob(strconv.Itoa(i))
		_, err := store.Write(ctx, intBlobEntry(int64(i), blobValue))
		should.Nil(err)
	}
	should.Nil(store.UpdateIndex(ctx))

	reader, err := store.NewReader(ctx)
	should.Nil(err)
	collector := &lstore.RowsCollector{}
	reader.SearchForward(ctx, &lstore.SearchRequest{
		0, nil, collector,
	})
	should.Equal(520, len(collector.Rows))
	for _, row := range collector.Rows {
		should.Equal(row.IntValues[0] + 1, int64(row.Offset))
	}
}

func Test_update_index_multiple_times(t *testing.T) {
	should := require.New(t)
	cfg := &lstore.Config{}
	cfg.BloomFilterIndexedBlobColumns = []int{0}
	store := testStore(cfg)
	defer store.Stop(ctx)
	for i := 0; i < 4096 * 64; i++ {
		blobValue := lstore.Blob(strconv.Itoa(i))
		offset, err := store.Write(ctx, intBlobEntry(int64(i), blobValue))
		should.Nil(err)
		should.Equal(lstore.Offset(i + 1), offset)
	}
	for i := 0; i < 65; i++ {
		should.Nil(store.UpdateIndex(ctx))
	}
	reader, err := store.NewReader(ctx)
	should.Nil(err)
	collector := &lstore.RowsCollector{}
	reader.SearchForward(ctx, &lstore.SearchRequest{
		0, nil, collector,
	})
	should.Equal(lstore.Offset(262144), collector.Rows[len(collector.Rows) - 1].Offset)
	should.Equal(262144, len(collector.Rows))
	for _, row := range collector.Rows {
		should.Equal(row.IntValues[0] + 1, int64(row.Offset))
	}
}

func Test_index_block_compressed(t *testing.T) {
	should := require.New(t)
	cfg := &lstore.Config{}
	cfg.BloomFilterIndexedBlobColumns = []int{0}
	cfg.BlockCompressed = true
	store := testStore(cfg)
	defer store.Stop(ctx)
	for i := 0; i < 260; i++ {
		blobValue := lstore.Blob("hello")
		if i%2 == 0 {
			blobValue = lstore.Blob("world")
		}
		_, err := store.Write(ctx, intBlobEntry(int64(i)+1, blobValue))
		should.Nil(err)
	}
	should.Nil(store.UpdateIndex(ctx))
	reader, err := store.NewReader(ctx)
	should.Nil(err)
	collector := &lstore.RowsCollector{LimitSize: 2}
	reader.SearchForward(ctx, &lstore.SearchRequest{
		0, store.NewBlobValueFilter(0, "hello"),
		&assertSearchForward{collector, 0},
	})
	should.Equal([]int64{2}, collector.Rows[0].IntValues)
	should.Equal([]int64{4}, collector.Rows[1].IntValues)
}
