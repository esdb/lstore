package test

import (
	"testing"
	"github.com/stretchr/testify/require"
	"context"
	"github.com/esdb/lstore"
)

func Test_compacted_segment(t *testing.T) {
	should := require.New(t)
	store := smallTestStore()
	defer store.Stop(context.Background())
	for j := 0; j < 10; j++ {
		for i := 0; i < 1000; i++ {
			blobValue := lstore.Blob("hello")
			if i%2 == 0 {
				blobValue = lstore.Blob("world")
			}
			_, err := store.Write(context.Background(), intBlobEntry(int64(i)+1, blobValue))
			should.Nil(err)
		}
		should.Nil(store.Index())
	}
	reader, err := store.NewReader()
	should.Nil(err)
	iter := reader.Search(context.Background(), lstore.SearchRequest{
		LimitSize: 2,
		Filters: []lstore.Filter{
			store.NewBlobValueFilter(0, "hello"),
		},
	})
	rows, err := iter()
	should.Nil(err)
	should.Equal([]int64{2}, rows[0].IntValues)
	should.Equal([]int64{4}, rows[1].IntValues)
}