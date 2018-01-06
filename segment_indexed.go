package lstore

import (
	"github.com/v2pro/plz/countlog"
	"github.com/esdb/biter"
	"io/ioutil"
	"github.com/esdb/gocodec"
	"fmt"
)

const levelsCount = 9
const level0 level = 0 // small
const level1 level = 1 // medium
const level2 level = 2 // large
type level int

// indexSegment can be serialized
type indexSegment struct {
	segmentHeader
	tailOffset       Offset
	tailBlockSeq     blockSeq
	tailSlotIndexSeq slotIndexSeq
	topLevel         level          // minimum 3 level
	levels           []slotIndexSeq // total 9 levels
}

// searchable makes the indexSegment searchable
type searchable struct {
	*indexSegment
	blockManager blockManager
	// readSlotIndex might read from rwCache or roCache, depending on
	readSlotIndex func(slotIndexSeq, level) (*slotIndex, error)
}

func openIndexedSegment(ctx countlog.Context, indexedSegmentPath string) (*indexSegment, error) {
	buf, err := ioutil.ReadFile(indexedSegmentPath)
	ctx.TraceCall("callee!ioutil.ReadFile", err)
	if err != nil {
		return nil, err
	}
	iter := gocodec.NewIterator(buf)
	segment, _ := iter.Unmarshal((*indexSegment)(nil)).(*indexSegment)
	if iter.Error != nil {
		return nil, fmt.Errorf("unmarshal index segment failed: %s", iter.Error.Error())
	}
	return segment, nil
}

func (segment *indexSegment) copy() *indexSegment {
	return &indexSegment{
		segmentHeader: segment.segmentHeader,
		tailOffset: segment.tailOffset,
		tailBlockSeq: segment.tailBlockSeq,
		tailSlotIndexSeq: segment.tailSlotIndexSeq,
		topLevel: segment.topLevel,
		levels: append([]slotIndexSeq(nil), segment.levels...),
	}
}

func (segment *searchable) searchForward(
	ctx countlog.Context, startOffset Offset, filter Filter, cb SearchCallback) error {
	if startOffset >= segment.tailOffset {
		return nil
	}
	slotIndex, err := segment.readSlotIndex(segment.levels[segment.topLevel], segment.topLevel)
	if err != nil {
		return err
	}
	return segment.searchForwardAt(ctx, startOffset, filter, slotIndex, segment.topLevel, cb)
}

func (segment *searchable) searchForwardAt(
	ctx countlog.Context, startOffset Offset, filter Filter,
	levelIndex *slotIndex, level level, cb SearchCallback) error {
	if level == level0 {
		for slot := biter.Slot(0); slot < levelIndex.tailSlot; slot++ {
			blk, err := segment.blockManager.readBlock(blockSeq(levelIndex.children[slot]))
			ctx.TraceCall("callee!blockManager.readBlock", err)
			if err != nil {
				return err
			}
			err = blk.scanForward(ctx, startOffset, filter, cb)
			if err != nil {
				return err
			}
		}
	} else {
		result := levelIndex.search(level, filter)
		result &= biter.SetBitsForwardUntil[levelIndex.tailSlot]
		iter := result.ScanForward()
		for {
			slot := iter()
			if slot == biter.NotFound {
				return nil
			}
			childLevel := level - 1
			childSlotIndexSeq := slotIndexSeq(levelIndex.children[slot])
			childSlotIndex, err := segment.readSlotIndex(childSlotIndexSeq, childLevel)
			ctx.TraceCall("callee!slotIndexManager.mapWritableSlotIndex", err)
			if err != nil {
				return err
			}
			err = segment.searchForwardAt(ctx, startOffset, filter, childSlotIndex, childLevel, cb)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
