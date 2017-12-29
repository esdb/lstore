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
	blk, blkHashCache := newBlock(strategy, []Row{
		{Seq: 1, Entry: &Entry{
			IntValues: []int64{1, 2},
			BlobValues: []Blob{"hello"},
		}},
		{Seq: 2, Entry: &Entry{
			IntValues: []int64{3, 4},
			BlobValues: []Blob{"world"},
		}},
	})
	should.Equal([]RowSeq{1, 2}, blk.seqColumn)
	should.Equal(intColumn{1, 3}, blk.intColumns[0])
	should.Equal(intColumn{2, 4}, blk.intColumns[1])
	should.Equal(blobColumn{"hello", "world"}, blk.blobColumns[0])
	should.Equal(blobHashColumn{0x4283d94f, 0x7283d94f}, blk.blobHashColumns[0])
	should.Equal(1, len(blkHashCache))
	should.Equal(hashColumn{
		{0xe3e1efd54283d94f, 0x7081314b599d31b3},
		{0xd81531287283d94f, 0x70816d9b61a08eeb},
	}, blkHashCache[0])
}