package lstore

import (
	"testing"
	"github.com/stretchr/testify/require"
)

func Test_create_block(t *testing.T) {
	should := require.New(t)
	strategy := newIndexingStrategy(&indexingStrategyConfig{
		BloomFilterIndexedBlobColumns: []int{0},
	})
	blk := newBlock([]Row{
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
	blkHash := blk.Hash(strategy)
	should.Equal(1, len(blkHash))
	should.Equal(hashColumn{
		{0xe3e1efd54283d94f, 0x7081314b599d31b3},
		{0xd81531287283d94f, 0x70816d9b61a08eeb},
	}, blkHash[0])
	should.Equal(blobHashColumn{0x4283d94f, 0x7283d94f}, blk.blobHashColumns[0])
}

func Benchmark_column_based_block_scan(b *testing.B) {
	filters := []Filter{
		&IntValueFilter{Index: 0, Value: 100},
	}
	columnSize := 256
	blk := &block{
		seqColumn:       make([]RowSeq, columnSize),
		intColumns:      []intColumn{make(intColumn, columnSize)},
		blobHashColumns: []blobHashColumn{make(blobHashColumn, columnSize)},
		blobColumns:     []blobColumn{make(blobColumn, columnSize)},
	}
	for i := 0; i < b.N; i++ {
		blk.search(nil, 0, filters, nil)
	}
}