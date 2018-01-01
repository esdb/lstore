package lstore

import (
	"testing"
	"github.com/stretchr/testify/require"
	"github.com/v2pro/plz/countlog"
	"context"
	"github.com/esdb/biter"
	"fmt"
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
		strategy:           strategy,
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
	filterHello := strategy.NewBlobValueFilter(0, "hello")
	result := level0SlotIndex.searchSmall(filterHello)
	should.Equal(biter.SetBits[0], result)
	result = editing.editedLevels[1].searchMedium(filterHello)
	should.Equal(biter.SetBits[0], result)
	result = editing.editedLevels[2].searchLarge(filterHello)
	should.Equal(biter.SetBits[0], result)
	filter123 := strategy.NewBlobValueFilter(0, "123")
	result = editing.editedLevels[2].searchLarge(filter123)
	should.Equal(biter.Bits(0), result)
}

func Test_add_two_blocks(t *testing.T) {
	should := require.New(t)
	editing := testEditingHead()
	editing.addBlock(ctx, newBlock(0, []*Entry{
		blobEntry("hello"),
	}))
	editing.addBlock(ctx, newBlock(0, []*Entry{
		blobEntry("world"),
	}))
	should.Equal(blockSeq(12), editing.tailBlockSeq)
	level0SlotIndex := editing.editedLevels[0]
	should.Equal([]uint64{0, 6}, level0SlotIndex.children)
	strategy := editing.strategy
	filterHello := strategy.NewBlobValueFilter(0, "hello")
	result := level0SlotIndex.searchSmall(filterHello)
	should.Equal(biter.SetBits[0], result)
	filterWorld := strategy.NewBlobValueFilter(0, "world")
	result = level0SlotIndex.searchSmall(filterWorld)
	should.Equal(biter.SetBits[1], result)
}

func Test_add_64_blocks(t *testing.T) {
	should := require.New(t)
	editing := testEditingHead()
	for i := 0; i< 64; i++ {
		editing.addBlock(ctx, newBlock(0, []*Entry{
			blobEntry(Blob(fmt.Sprintf("hello%d", i))),
		}))
	}
	should.Equal(blockSeq(6 * 64), editing.tailBlockSeq)
	level0SlotIndex := editing.editedLevels[0]
	strategy := editing.strategy
	result := level0SlotIndex.searchSmall(strategy.NewBlobValueFilter(0, "hello0"))
	should.Equal(biter.SetBits[0], result & biter.SetBits[0])
	result = level0SlotIndex.searchSmall(strategy.NewBlobValueFilter(0, "hello63"))
	should.Equal(biter.SetBits[63], result & biter.SetBits[63])
}
