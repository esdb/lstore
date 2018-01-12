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