package lstore

import (
	"github.com/v2pro/plz/countlog"
	"github.com/esdb/biter"
	"github.com/esdb/pbloom"
)

type indexingChunk struct {
	*indexSegment
	indexingLevels   []*slotIndex
	blockManager     blockManager
	slotIndexManager slotIndexManager
	strategy         *IndexingStrategy
}

func (chunk *indexingChunk) addBlock(ctx countlog.Context, blk *block) error {
	var err error
	slots, err := chunk.nextSlot(ctx)
	level0SlotMask := biter.SetBits[slots[level0]]
	level1SlotMask := biter.SetBits[slots[level1]]
	level2SlotMask := biter.SetBits[slots[level2]]
	if err != nil {
		return err
	}
	// hash will update block, so call it before write
	blkHash := blk.Hash(chunk.strategy)
	var blockSeq blockSeq
	blockSeq, chunk.tailBlockSeq, err = chunk.blockManager.writeBlock(chunk.tailBlockSeq, blk)
	ctx.TraceCall("callee!chunk.writeBlock", err)
	if err != nil {
		return err
	}
	level0SlotIndex := chunk.indexingLevels[0]
	level1SlotIndex := chunk.indexingLevels[1]
	level2SlotIndex := chunk.indexingLevels[2]
	level0SlotIndex.children[slots[0]] = uint64(blockSeq)
	level0SlotIndex.tailSlot = slots[0] + 1
	for i, hashColumn := range blkHash {
		pbf0 := level0SlotIndex.pbfs[i]
		pbf1 := level1SlotIndex.pbfs[i]
		pbf2 := level2SlotIndex.pbfs[i]
		for _, hashedElement := range hashColumn {
			// level0, level1, level2 are computed from block hash
			locations := pbloom.BatchPut(hashedElement,
				level0SlotMask, level1SlotMask, level2SlotMask,
				pbf0, pbf1, pbf2)
			// from level3 to levelN, they are derived from level2
			for j := level(3); j <= chunk.topLevel; j++ {
				parentPbf := chunk.indexingLevels[j].pbfs[i]
				levelNMask := biter.SetBits[slots[j]]
				parentPbf[locations[0]] |= levelNMask
				parentPbf[locations[1]] |= levelNMask
				parentPbf[locations[2]] |= levelNMask
				parentPbf[locations[3]] |= levelNMask
			}
		}
	}
	chunk.tailOffset += Offset(blockLength)
	return nil
}

func (chunk *indexingChunk) nextSlot(ctx countlog.Context) ([]biter.Slot, error) {
	slots := make([]biter.Slot, 9)
	cnt := int(chunk.tailOffset-chunk.headOffset) >> blockLengthInPowerOfTwo
	level0Slot := biter.Slot(cnt % 64)
	cnt = cnt >> 6
	level1Slot := biter.Slot(cnt % 64)
	cnt = cnt >> 6
	level2Slot := biter.Slot(cnt % 64)
	cnt = cnt >> 6
	level3Slot := biter.Slot(cnt % 64)
	cnt = cnt >> 6
	level4Slot := biter.Slot(cnt % 64)
	cnt = cnt >> 6
	level5Slot := biter.Slot(cnt % 64)
	cnt = cnt >> 6
	level6Slot := biter.Slot(cnt % 64)
	cnt = cnt >> 6
	level7Slot := biter.Slot(cnt % 64)
	cnt = cnt >> 6
	level8Slot := biter.Slot(cnt % 64)
	slots = []biter.Slot{level0Slot, level1Slot, level2Slot, level3Slot, level4Slot,
		level5Slot, level6Slot, level7Slot, level8Slot}
	if level0Slot != 0 || (chunk.tailOffset-chunk.headOffset) == 0 {
		return slots, nil
	}
	if level1Slot != 0 {
		if err := chunk.rotate(level0, level1Slot-1); err != nil {
			return nil, err
		}
		return slots, nil
	}
	if err := chunk.rotate(level0, 63); err != nil {
		return nil, err
	}
	if level2Slot != 0 {
		if err := chunk.rotate(level1, level2Slot-1); err != nil {
			return nil, err
		}
		return slots, nil
	}
	if err := chunk.rotate(level1, 63); err != nil {
		return nil, err
	}
	if level3Slot != 0 {
		if err := chunk.rotate(level2, level3Slot-1); err != nil {
			return nil, err
		}
		return slots, nil
	}
	if err := chunk.rotate(level2, 63); err != nil {
		return nil, err
	}
	if level4Slot != 0 {
		if err := chunk.rotate(3, level4Slot-1); err != nil {
			return nil, err
		}
		return slots, nil
	}
	if err := chunk.rotate(3, 63); err != nil {
		return nil, err
	}
	if level5Slot != 0 {
		if err := chunk.rotate(4, level5Slot-1); err != nil {
			return nil, err
		}
		return slots, nil
	}
	if err := chunk.rotate(4, 63); err != nil {
		return nil, err
	}
	if level6Slot != 0 {
		if err := chunk.rotate(5, level6Slot-1); err != nil {
			return nil, err
		}
		return slots, nil
	}
	if err := chunk.rotate(5, 63); err != nil {
		return nil, err
	}
	if level7Slot != 0 {
		if err := chunk.rotate(6, level7Slot-1); err != nil {
			return nil, err
		}
		return slots, nil
	}
	if err := chunk.rotate(6, 63); err != nil {
		return nil, err
	}
	if level8Slot != 0 {
		if err := chunk.rotate(7, level8Slot-1); err != nil {
			return nil, err
		}
		return slots, nil
	}
	panic("bloom filter tree exceed capacity")
}

func (chunk *indexingChunk) rotate(level level, slot biter.Slot) (err error) {
	if level+1 > chunk.topLevel {
		chunk.topLevel = level + 1
		slotIndexSeq, nextSlotIndexSeq, slotIndex, err := chunk.slotIndexManager.newSlotIndex(
			chunk.tailSlotIndexSeq, level)
		if err != nil {
			return err
		}
		chunk.tailSlotIndexSeq = nextSlotIndexSeq
		chunk.levels[level+1] = slotIndexSeq
		chunk.indexingLevels[level+1] = slotIndex
		slotIndex.updateSlot(biter.SetBits[0], chunk.indexingLevels[level])
		slotIndex.tailSlot = 1
	}
	parentLevel := chunk.indexingLevels[level+1]
	parentLevel.children[slot] = uint64(chunk.levels[level])
	parentLevel.tailSlot = slot + 1
	slotIndexManager := chunk.slotIndexManager
	if err := slotIndexManager.updateChecksum(chunk.levels[level], level); err != nil {
		return err
	}
	chunk.levels[level], chunk.tailSlotIndexSeq, chunk.indexingLevels[level], err = slotIndexManager.newSlotIndex(
		chunk.tailSlotIndexSeq, level)
	return
}
