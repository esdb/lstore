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
	for i := 0; i < 4; i++ {
		_, err := store.Write(context.Background(), intEntry(int64(i)+1))
		should.Nil(err)
	}
	should.Nil(store.Compact())
	reader, err := store.NewReader()
	should.Nil(err)
	iter := reader.Search(context.Background(), lstore.SearchRequest{
		LimitSize: 1,
	})
	rows, err := iter()
	should.Nil(err)
	should.Equal([]int64{1}, rows[0].IntValues)
}
