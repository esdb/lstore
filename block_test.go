package lstore

import (
	"testing"
	"github.com/stretchr/testify/require"
)

func Test_create_block(t *testing.T) {
	should := require.New(t)
	strategy := NewIndexingStrategy(IndexingStrategyConfig{
		BloomFilterIndexedBlobColumns: []int{0},
	})
	blk := newBlock(0, []*Entry{{
		IntValues:  []int64{1, 2},
		BlobValues: []Blob{"hello"},
	}, {
		IntValues:  []int64{3, 4},
		BlobValues: []Blob{"world"},
	}})
	should.Equal(0, blk.startOffset)
	should.Equal(intColumn{1, 3}, blk.intColumns[0])
	should.Equal(intColumn{2, 4}, blk.intColumns[1])
	should.Equal(blobColumn{"hello", "world"}, blk.blobColumns[0])
	blkHash := blk.Hash(strategy)
	should.Equal(1, len(blkHash))
	should.Equal(hashColumn{
	}, blkHash[0])
	should.Equal(blobHashColumn{0x4283d94f, 0x7283d94f}, blk.blobHashColumns[0])
}
