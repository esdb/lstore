package lstore

import (
	"testing"
	"github.com/stretchr/testify/require"
	"github.com/v2pro/plz/countlog"
	"context"
	"github.com/esdb/biter"
)


func intEntry(values ...int64) *Entry {
	return &Entry{EntryType: EntryTypeData, IntValues: values}
}

func blobEntry(values ...Blob) *Entry {
	return &Entry{EntryType: EntryTypeData, BlobValues: values}
}

func intBlobEntry(intValue int64, blobValue Blob) *Entry {
	return &Entry{EntryType: EntryTypeData, IntValues: []int64{intValue}, BlobValues: []Blob{blobValue}}
}

var ctx = countlog.Ctx(context.Background())

func testEditingHead() *editingHead {
	strategy := newIndexingStrategy(&indexingStrategyConfig{
		BloomFilterIndexedBlobColumns: []int{0},
	})
	var levels []*indexingSegment
	for level := level0; level < 9; level++ {
		levels = append(levels, &indexingSegment{
			indexingSegmentVersion: indexingSegmentVersion{
				slotIndex: newSlotIndex(strategy, strategy.hashingStrategy(level)),
			},
		})
	}
	return &editingHead{
		strategy: strategy,
		headSegmentVersion: &headSegmentVersion{},
		writeBlock: func(seq blockSeq, block *block) (blockSeq, error) {
			return seq + 6, nil
		},
		levels: levels,
	}
}

func Test_add_first_block(t *testing.T) {
	should := require.New(t)
	editing := testEditingHead()
	editing.addBlock(ctx, newBlock(0, []*Entry{
		blobEntry("hello"),
	}))
	should.Equal(blockSeq(6), editing.tailBlockSeq)
	level0SlotIndex := editing.editedLevels[0]
	should.Equal([]uint64{0}, level0SlotIndex.children)
	strategy := editing.strategy
	filter := strategy.NewBlobValueFilter(0, "hello")
	result := level0SlotIndex.searchSmall([]Filter{filter})
	should.Equal(biter.SetBits[0], result)
}