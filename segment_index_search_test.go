package lstore

import (
	"testing"
	"github.com/stretchr/testify/require"
)

func Test_search_index_segment(t *testing.T) {
	should := require.New(t)
	segment := testIndexSegment()
	segment.addBlock(ctx, segment.slotIndexWriter, segment.blockWriter, newBlock(0, []*Entry{
		blobEntry("hello"),
	}))
	collector := &OffsetsCollector{}
	segment.searchForward(ctx, segment.slotIndexReader, segment.blockReader, &SearchRequest{
		0, dummyFilterInstance, collector,
	})
	should.Equal(blockLength, len(collector.Offsets))
}

func Test_search_index_segment_with_16640(t *testing.T) {
	should := require.New(t)
	segment := testIndexSegment()
	for i := 0; i < 64; i++ {
		segment.addBlock(ctx, segment.slotIndexWriter, segment.blockWriter, newBlock(0, []*Entry{
			blobEntry("hello"),
		}))
	}
	segment.addBlock(ctx, segment.slotIndexWriter, segment.blockWriter, newBlock(0, []*Entry{
		blobEntry("hello"),
	}))
	collector := &OffsetsCollector{}
	segment.searchForward(ctx, segment.slotIndexReader, segment.blockReader, &SearchRequest{
		0, dummyFilterInstance, collector,
	})
	should.Equal(blockLength * 65, len(collector.Offsets))
}