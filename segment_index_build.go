package lstore

import (
	"github.com/v2pro/plz/countlog"
	"github.com/esdb/biter"
	"github.com/esdb/pbloom"
)

func (segment *indexSegment) addBlock(ctx countlog.Context,
	slotIndexWriter slotIndexWriter, blockWriter blockWriter, blk *block) error {
	// hash will update block, so call it before write
	blkHash := blk.Hash(slotIndexWriter.indexingStrategy())
	var blockSeq blockSeq
	var err error
	blockSeq, segment.tailBlockSeq, err = blockWriter.writeBlock(segment.tailBlockSeq, blk)
	ctx.TraceCall("callee!segment.writeBlock", err)
	if err != nil {
		return err
	}
	indices := make([]*slotIndex, levelsCount)
	masks := make([]biter.Bits, levelsCount)
	shouldMoveUp := true
	for i := level0; i <= segment.topLevel; i++ {
		index, err := slotIndexWriter.mapWritableSlotIndex(segment.levels[i], i)
		if err != nil {
			return err
		}
		indices[i] = index
		if i == level0 {
			masks[i] = biter.SetBits[*index.tailSlot]
		} else {
			masks[i] = biter.SetBits[*index.tailSlot - 1]
		}
		if *index.tailSlot != 63 {
			shouldMoveUp = false
		}
	}
	if shouldMoveUp {
		segment.topLevel += 1
		var parent *slotIndex
		segment.levels[segment.topLevel], segment.tailSlotIndexSeq, parent, err = slotIndexWriter.newSlotIndex(
			segment.tailSlotIndexSeq, segment.topLevel)
		parent.children[0] = uint64(segment.levels[segment.topLevel - 1])
		parent.setTailSlot(1)
		indices[segment.topLevel] = parent
	}
	level0Index := indices[0]
	level1Index := indices[1]
	level2Index := indices[2]
	level0SlotMask := masks[0]
	level1SlotMask := masks[1]
	level2SlotMask := masks[2]
	level0Index.children[*level0Index.tailSlot] = uint64(blockSeq)
	for i, hashColumn := range blkHash {
		pbf0 := level0Index.pbfs[i]
		pbf1 := level1Index.pbfs[i]
		pbf2 := level2Index.pbfs[i]
		for _, hashedElement := range hashColumn {
			// level0, level1, level2 are computed from block hash
			locations := pbloom.BatchPut(hashedElement,
				level0SlotMask, level1SlotMask, level2SlotMask,
				pbf0, pbf1, pbf2)
			// from level3 to levelN, they are derived from level2
			for j := level(3); j <= segment.topLevel; j++ {
				parentPbf := indices[j].pbfs[i]
				levelNMask := masks[j]
				parentPbf[locations[0]] |= levelNMask
				parentPbf[locations[1]] |= levelNMask
				parentPbf[locations[2]] |= levelNMask
				parentPbf[locations[3]] |= levelNMask
			}
		}
	}
	segment.tailOffset += Offset(blockLength)
	return segment.nextSlot(ctx, slotIndexWriter, indices)
}

func (segment *indexSegment) nextSlot(ctx countlog.Context, slotIndexWriter slotIndexWriter, indices []*slotIndex) error {
	level0index := indices[0]
	newTailSlot := *level0index.tailSlot + 1
	level0index.setTailSlot(newTailSlot)
	if newTailSlot == 64 {
		_, err := segment.rotate(slotIndexWriter, level0, indices)
		return err
	}
	return nil
}

func (segment *indexSegment) rotate(slotIndexWriter slotIndexWriter, level level, indices []*slotIndex) (slotIndex *slotIndex, err error) {
	segment.levels[level], segment.tailSlotIndexSeq, slotIndex, err = slotIndexWriter.newSlotIndex(
		segment.tailSlotIndexSeq, level)
	parent := indices[level+1]
	parentTailSlot := *parent.tailSlot
	if parentTailSlot == 64 {
		parent, err = segment.rotate(slotIndexWriter, level+1, indices)
		if err != nil {
			return nil, err
		}
		parent.setTailSlot(1)
		parent.children[0] = uint64(segment.levels[level])
	} else {
		parent.setTailSlot(parentTailSlot + 1)
		parent.children[parentTailSlot] = uint64(segment.levels[level])
	}
	return slotIndex, nil
}
