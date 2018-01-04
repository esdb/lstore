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
	should.Equal(Offset(0), blk.startOffset)
	should.Equal(intColumn{1, 3}, blk.intColumns[0][:2])
	should.Equal(intColumn{2, 4}, blk.intColumns[1][:2])
	should.Equal(blobColumn{"hello", "world"}, blk.blobColumns[0][:2])
	blkHash := blk.Hash(strategy)
	should.Equal(1, len(blkHash))
}

func Test_block_search(t *testing.T) {
	should := require.New(t)

	type TestCase struct {
		input  []*Entry
		filter Blob
		output []Offset
	}
	testCases := []TestCase{
		{
			input: []*Entry{{
				IntValues:  []int64{1, 2},
				BlobValues: []Blob{"hello"},
			}, {
				IntValues:  []int64{1, 2},
				BlobValues: []Blob{"world"},
			}, {
				IntValues:  []int64{1, 2},
				BlobValues: []Blob{"world"},
			}},
			filter: "world",
			output: []Offset{1, 2},
		},
	}
	for _, testCase := range testCases {
		blk := newBlock(0, testCase.input)
		strategy := NewIndexingStrategy(IndexingStrategyConfig{
			BloomFilterIndexedBlobColumns: []int{0},
		})
		blk.Hash(strategy)
		filter := strategy.NewBlobValueFilter(0, testCase.filter)
		collector := &OffsetsCollector{}
		blk.scanForward(ctx, 0, []Filter{
			filter,
		}, collector)
		should.Equal(testCase.output, collector.Offsets)
	}
}
