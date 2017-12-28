package test

import (
	"testing"
	"context"
	"github.com/stretchr/testify/require"
)

func Test_block_segment(t *testing.T) {
	should := require.New(t)
	store := smallTestStore()
	defer store.Stop(context.Background())
	for i := 0; i < 4; i++ {
		_, err := store.Write(context.Background(), intEntry(int64(i)+1))
		should.Nil(err)
	}
	store.Compact()
}