package test

import (
	"testing"
	"github.com/stretchr/testify/require"
	"github.com/esdb/lstore"
	"strconv"
)

func Test_indexing_segment(t *testing.T) {
	should := require.New(t)
	store := smallTestStore()
	defer store.Stop(ctx)
	for i := 0; i < 260; i++ {
		blobValue := lstore.Blob("hello")
		if i%2 == 0 {
			blobValue = lstore.Blob("world")
		}
		_, err := store.Write(ctx, intBlobEntry(int64(i)+1, blobValue))
		should.Nil(err)
	}
	should.Nil(store.Index())
	reader, err := store.NewReader(ctx)
	should.Nil(err)
	collector := &lstore.RowsCollector{LimitSize: 2}
	reader.SearchForward(ctx, 0, store.IndexingStrategy.NewBlobValueFilter(0, "hello"), collector)
	should.Equal([]int64{2}, collector.Rows[0].IntValues)
	should.Equal([]int64{4}, collector.Rows[1].IntValues)
}

func Test_reopen_indexing_segment(t *testing.T) {
	should := require.New(t)
	store := bigTestStore()
	defer store.Stop(ctx)
	for i := 0; i < 260; i++ {
		blobValue := lstore.Blob("hello")
		if i%2 == 0 {
			blobValue = lstore.Blob("world")
		}
		_, err := store.Write(ctx, intBlobEntry(int64(i)+1, blobValue))
		should.Nil(err)
	}
	should.Nil(store.Index())

	store = reopenTestStore(store)

	reader, err := store.NewReader(ctx)
	should.Nil(err)
	collector := &lstore.RowsCollector{LimitSize: 2}
	reader.SearchForward(ctx, 0, store.IndexingStrategy.NewBlobValueFilter(0, "hello"), collector)
	should.Equal([]int64{2}, collector.Rows[0].IntValues)
	should.Equal([]int64{4}, collector.Rows[1].IntValues)
}

func Test_index_twice_should_not_repeat_rows(t *testing.T) {
	should := require.New(t)
	store := bigTestStore()
	defer store.Stop(ctx)
	for i := 0; i < 260; i++ {
		blobValue := lstore.Blob(strconv.Itoa(i))
		offset, err := store.Write(ctx, intBlobEntry(int64(i), blobValue))
		should.Nil(err)
		should.Equal(lstore.Offset(i), offset)
	}
	should.Nil(store.Index())
	for i := 260; i < 520; i++ {
		blobValue := lstore.Blob(strconv.Itoa(i))
		_, err := store.Write(ctx, intBlobEntry(int64(i), blobValue))
		should.Nil(err)
	}
	should.Nil(store.Index())

	reader, err := store.NewReader(ctx)
	should.Nil(err)
	collector := &lstore.RowsCollector{}
	reader.SearchForward(ctx, 0, nil,collector)
	should.Equal(520, len(collector.Rows))
	for _, row := range collector.Rows {
		should.Equal(row.IntValues[0], int64(row.Offset))
	}
}
//
//func Test_a_lot_indexed_segment(t *testing.T) {
//	should := require.New(t)
//	store := smallTestStore()
//	defer store.Stop(ctx)
//	for i := 0; i < 160; i++ {
//		blobValue := lstore.Blob("hello")
//		if i%2 == 0 {
//			blobValue = lstore.Blob("world")
//		}
//		_, err := store.Write(ctx, intBlobEntry(int64(i)+1, blobValue))
//		should.Nil(err)
//	}
//	should.Nil(store.Index())
//	reader, err := store.NewReader(ctx)
//	should.Nil(err)
//	iter := reader.Search(ctx, lstore.SearchRequest{
//		LimitSize: 2,
//		Filters: []lstore.Filter{
//			store.IndexingStrategy.NewBlobValueFilter(0, "hello"),
//		},
//	})
//	rows, err := iter()
//	should.Nil(err)
//	should.Equal([]int64{2}, rows[0].IntValues)
//	should.Equal([]int64{4}, rows[1].IntValues)
//}
//
//func Test_compacted_segment(t *testing.T) {
//	should := require.New(t)
//	store := smallTestStore()
//	defer store.Stop(ctx)
//	for j := 0; j < 10; j++ {
//		for i := 0; i < 1000; i++ {
//			blobValue := lstore.Blob("hello")
//			if i%2 == 0 {
//				blobValue = lstore.Blob("world")
//			}
//			_, err := store.Write(ctx, intBlobEntry(int64(i)+1, blobValue))
//			should.Nil(err)
//		}
//		should.Nil(store.Index())
//	}
//	reader, err := store.NewReader(ctx)
//	should.Nil(err)
//	iter := reader.Search(ctx, lstore.SearchRequest{
//		LimitSize: 2,
//		Filters: []lstore.Filter{
//			store.IndexingStrategy.NewBlobValueFilter(0, "hello"),
//		},
//	})
//	rows, err := iter()
//	should.Nil(err)
//	should.Equal([]int64{2}, rows[0].IntValues)
//	should.Equal([]int64{4}, rows[1].IntValues)
//}