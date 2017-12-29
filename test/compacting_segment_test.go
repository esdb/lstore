package test

import (
	"testing"
	"context"
	"github.com/stretchr/testify/require"
	"github.com/esdb/lstore"
)

func Test_compacting_segment(t *testing.T) {
	should := require.New(t)
	store := smallTestStore()
	defer store.Stop(context.Background())
	for i := 0; i < 16; i++ {
		blobValue := lstore.Blob("hello")
		if i%2 == 0 {
			blobValue = lstore.Blob("world")
		}
		_, err := store.Write(context.Background(), intBlobEntry(int64(i)+1, blobValue))
		should.Nil(err)
	}
	should.Nil(store.Compact())
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
