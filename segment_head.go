package lstore

import (
	"github.com/esdb/lstore/ref"
	"io/ioutil"
	"github.com/esdb/gocodec"
	"os"
	"path"
	"io"
	"github.com/edsrzf/mmap-go"
	"github.com/v2pro/plz/countlog"
	"github.com/v2pro/plz"
	"github.com/esdb/biter"
)

const levelsCount = 9
const level0 level = 0 // small
const level1 level = 1 // medium
const level2 level = 2 // large
type level int

type headSegmentVersion struct {
	segmentHeader
	headOffset       Offset
	tailOffset       Offset
	tailBlockSeq     blockSeq
	tailSlotIndexSeq slotIndexSeq
	topLevel         level          // minimum 3 level
	levels           []slotIndexSeq // total 9 levels
}

type headSegment struct {
	*headSegmentVersion
	*ref.ReferenceCounted
	writeMMap mmap.MMap
}

// TODO: merge editingHead into headSegment
type editingHead struct {
	*headSegmentVersion
	writeBlock       func(blockSeq, *block) (blockSeq, blockSeq, error)
	slotIndexManager *slotIndexManager
	strategy         *IndexingStrategy
}

type editingLevel struct {
	slotIndex *slotIndex
	slot      biter.Slot
}

func openHeadSegment(ctx countlog.Context, segmentPath string, slotIndexManager *slotIndexManager) (*headSegment, error) {
	file, err := os.OpenFile(segmentPath, os.O_RDWR, 0666)
	if os.IsNotExist(err) {
		file, err = initHeadSegment(ctx, segmentPath, slotIndexManager)
	}
	ctx.TraceCall("callee!os.OpenFile", err)
	if err != nil {
		return nil, err
	}
	resources := []io.Closer{file}
	writeMMap, err := mmap.Map(file, mmap.RDWR, 0)
	ctx.TraceCall("callee!mmap.Map", err)
	if err != nil {
		return nil, err
	}
	resources = append(resources, plz.WrapCloser(writeMMap.Unmap))
	iter := gocodec.NewIterator(writeMMap)
	segment, _ := iter.Unmarshal((*headSegmentVersion)(nil)).(*headSegmentVersion)
	ctx.TraceCall("callee!iter.Unmarshal", iter.Error)
	if iter.Error != nil {
		return nil, iter.Error
	}
	gocodec.UpdateChecksum(writeMMap)
	return &headSegment{
		headSegmentVersion: segment,
		ReferenceCounted:   ref.NewReferenceCounted("indexed segment", resources...),
		writeMMap:          writeMMap,
	}, nil
}

func initHeadSegment(ctx countlog.Context, segmentPath string, slotIndexManager *slotIndexManager) (*os.File, error) {
	levels := make([]slotIndexSeq, levelsCount)
	tailSlotIndexSeq := slotIndexSeq(0)
	for i := level0; i <= level2; i++ {
		var err error
		levels[i], tailSlotIndexSeq, _, err = slotIndexManager.newSlotIndex(tailSlotIndexSeq, i)
		if err != nil {
			return nil, err
		}
		err = slotIndexManager.flush(levels[i], i)
		if err != nil {
			return nil, err
		}
	}
	stream := gocodec.NewStream(nil)
	stream.Marshal(headSegmentVersion{
		segmentHeader: segmentHeader{segmentType: segmentTypeHead},
		topLevel:      level2,
		levels:        levels,
	})
	ctx.TraceCall("callee!stream.Marshal", stream.Error)
	if stream.Error != nil {
		return nil, stream.Error
	}
	os.MkdirAll(path.Dir(segmentPath), 0777)
	err := ioutil.WriteFile(segmentPath, stream.Buffer(), 0666)
	ctx.TraceCall("callee!ioutil.WriteFile", err)
	if err != nil {
		return nil, err
	}
	return os.OpenFile(segmentPath, os.O_RDWR, 0666)
}

func (editing *editingHead) addBlock(ctx countlog.Context, blk *block) error {
	var err error
	editingLevels, err := editing.nextSlot(ctx)
	level0SlotMask := biter.SetBits[editingLevels[level0].slot]
	level1SlotMask := biter.SetBits[editingLevels[level1].slot]
	level2SlotMask := biter.SetBits[editingLevels[level2].slot]
	if err != nil {
		return err
	}
	// hash will update block, so call it before write
	blkHash := blk.Hash(editing.strategy)
	var blockSeq blockSeq
	blockSeq, editing.tailBlockSeq, err = editing.writeBlock(blockSeq, blk)
	ctx.TraceCall("callee!editing.writeBlock", err)
	if err != nil {
		return err
	}
	level0SlotIndex := editingLevels[0].slotIndex
	level1SlotIndex := editingLevels[1].slotIndex
	level2SlotIndex := editingLevels[2].slotIndex
	level0SlotIndex.children[editingLevels[0].slot] = uint64(blockSeq)
	smallHashingStrategy := editing.strategy.smallHashingStrategy
	mediumHashingStrategy := editing.strategy.mediumHashingStrategy
	largeHashingStrategy := editing.strategy.largeHashingStrategy
	for i, hashColumn := range blkHash {
		pbf0 := level0SlotIndex.pbfs[i]
		pbf1 := level1SlotIndex.pbfs[i]
		pbf2 := level2SlotIndex.pbfs[i]
		for _, hashedElement := range hashColumn {
			// level0, level1, level2 are computed from block hash
			pbf0.Put(level0SlotMask, smallHashingStrategy.HashStage2(hashedElement))
			pbf1.Put(level1SlotMask, mediumHashingStrategy.HashStage2(hashedElement))
			locations := largeHashingStrategy.HashStage2(hashedElement)
			pbf2.Put(level2SlotMask, locations)
			// from level3 to levelN, they are derived from level2
			for j := level(3); j <= editing.topLevel; j++ {
				parentPbf := editingLevels[j].slotIndex.pbfs[i]
				levelNMask := biter.SetBits[editingLevels[j].slot]
				for _, location := range locations {
					parentPbf[location] |= levelNMask
				}
			}
		}
	}
	editing.tailOffset += Offset(blockLength)
	return nil
}

func (editing *editingHead) nextSlot(ctx countlog.Context) ([levelsCount]editingLevel, error) {
	var editingLevels [levelsCount]editingLevel
	level0SlotIndex, err := editing.editLevel(level0)
	if err != nil {
		return editingLevels, err
	}
	level1SlotIndex, err := editing.editLevel(level1)
	if err != nil {
		return editingLevels, err
	}
	level2SlotIndex, err := editing.editLevel(level2)
	if err != nil {
		return editingLevels, err
	}
	level0Slots := int(editing.tailOffset) >> blockLengthInPowerOfTwo
	level0Slot := biter.Slot(level0Slots % 64)
	level1Slots := level0Slots >> 6
	level1Slot := biter.Slot(level1Slots % 64)
	level2Slots := level1Slots >> 6
	level2Slot := biter.Slot(level2Slots % 64)
	editingLevels[level0] = editingLevel{level0SlotIndex, level0Slot}
	editingLevels[level1] = editingLevel{level1SlotIndex, level1Slot}
	editingLevels[level2] = editingLevel{level2SlotIndex, level2Slot}
	shouldExpand := level0Slot == 0 && level0Slots > 0
	if !shouldExpand {
		return editingLevels, nil
	}
	//var err error
	//level0SlotIndexSeq := uint64(editing.tailSlotIndexSeq)
	//editing.tailSlotIndexSeq, err = editing.writeSlotIndex(editing.tailSlotIndexSeq, level0SlotIndex)
	//ctx.TraceCall("callee!editing.writeSlotIndex", err)
	//if err != nil {
	//	return 0, 0, 0, 0, err
	//}
	//level1SlotIndex := editing.editLevel(level1)
	//shouldExpand = level1Slot == 0
	//if !shouldExpand {
	//	level1SlotIndex.children[level1Slot-1] = level0SlotIndexSeq
	//	editing.levels[level0] = newSlotIndex(editing.strategy, level0)
	//	return level0Slot, level1Slot, level2Slot, level2Slots, nil
	//}
	//level1SlotIndex.children[63] = level0SlotIndexSeq
	//editing.levels[level0] = newSlotIndex(editing.strategy, level0)
	//level1SlotIndexSeq := uint64(editing.tailSlotIndexSeq)
	//editing.tailSlotIndexSeq, err = editing.writeSlotIndex(editing.tailSlotIndexSeq, level1SlotIndex)
	//ctx.TraceCall("callee!editing.writeSlotIndex", err)
	//if err != nil {
	//	return 0, 0, 0, 0, err
	//}
	//level2SlotIndex := editing.editLevel(level2)
	//shouldExpand = level2Slot == 0
	//if !shouldExpand {
	//	level2SlotIndex.children[level2Slot-1] = level1SlotIndexSeq
	//	editing.levels[level1] = newSlotIndex(editing.strategy, level1)
	//	return level0Slot, level1Slot, level2Slot, level2Slots, nil
	//}
	//level2SlotIndex.children[63] = level1SlotIndexSeq
	//editing.levels[level1] = newSlotIndex(editing.strategy, level1)
	//level2SlotIndexSeq := uint64(editing.tailSlotIndexSeq)
	//editing.tailSlotIndexSeq, err = editing.writeSlotIndex(editing.tailSlotIndexSeq, level2SlotIndex)
	//ctx.TraceCall("callee!editing.writeSlotIndex", err)
	//if err != nil {
	//	return 0, 0, 0, 0, err
	//}
	//justMovedTopLevel := editing.topLevel < 3
	//level3SlotIndex := editing.editLevel(3)
	//if justMovedTopLevel {
	//	level3SlotIndex.updateSlot(biter.SetBits[0], level2SlotIndex)
	//}
	//level3Slots := level2Slots >> 6
	//level3Slot := level3Slots % 64
	//shouldExpand = level3Slot == 0
	//if !shouldExpand {
	//	level3SlotIndex.children[level3Slot-1] = level2SlotIndexSeq
	//	editing.levels[level2] = newSlotIndex(editing.strategy, level2)
	//	return level0Slot, level1Slot, level2Slot, level2Slots, nil
	//}
	//level3SlotIndex.children[63] = level2SlotIndexSeq
	//editing.levels[level2] = newSlotIndex(editing.strategy, level2)
	//level3SlotIndexSeq := uint64(editing.tailSlotIndexSeq)
	//editing.tailSlotIndexSeq, err = editing.writeSlotIndex(editing.tailSlotIndexSeq, level3SlotIndex)
	//ctx.TraceCall("callee!editing.writeSlotIndex", err)
	//if err != nil {
	//	return 0, 0, 0, 0, err
	//}
	//justMovedTopLevel = editing.topLevel < 4
	//level4SlotIndex := editing.editLevel(4)
	//if justMovedTopLevel {
	//	level4SlotIndex.updateSlot(biter.SetBits[0], level3SlotIndex)
	//}
	//level4Slots := level3Slots >> 6
	//level4Slot := level4Slots % 64
	//shouldExpand = level4Slot == 0
	//if !shouldExpand {
	//	level4SlotIndex.children[level4Slot-1] = level3SlotIndexSeq
	//	editing.levels[3] = newSlotIndex(editing.strategy, 3)
	//	return level0Slot, level1Slot, level2Slot, level2Slots, nil
	//}
	//level4SlotIndex.children[63] = level3SlotIndexSeq
	//editing.levels[3] = newSlotIndex(editing.strategy, 3)
	//level4SlotIndexSeq := uint64(editing.tailSlotIndexSeq)
	//editing.tailSlotIndexSeq, err = editing.writeSlotIndex(editing.tailSlotIndexSeq, level4SlotIndex)
	//ctx.TraceCall("callee!editing.writeSlotIndex", err)
	//if err != nil {
	//	return 0, 0, 0, 0, err
	//}
	//justMovedTopLevel = editing.topLevel < 5
	//level5SlotIndex := editing.editLevel(5)
	//if justMovedTopLevel {
	//	level5SlotIndex.updateSlot(biter.SetBits[0], level4SlotIndex)
	//}
	//level5Slots := level4Slots >> 6
	//level5Slot := level5Slots % 64
	//shouldExpand = level5Slot == 0
	//if !shouldExpand {
	//	level5SlotIndex.children[level5Slot-1] = level4SlotIndexSeq
	//	editing.levels[4] = newSlotIndex(editing.strategy, 4)
	//	return level0Slot, level1Slot, level2Slot, level2Slots, nil
	//}
	//level5SlotIndex.children[63] = level4SlotIndexSeq
	//editing.levels[4] = newSlotIndex(editing.strategy, 4)
	//level5SlotIndexSeq := uint64(editing.tailSlotIndexSeq)
	//editing.tailSlotIndexSeq, err = editing.writeSlotIndex(editing.tailSlotIndexSeq, level5SlotIndex)
	//ctx.TraceCall("callee!editing.writeSlotIndex", err)
	//if err != nil {
	//	return 0, 0, 0, 0, err
	//}
	//justMovedTopLevel = editing.topLevel < 6
	//level6SlotIndex := editing.editLevel(6)
	//if justMovedTopLevel {
	//	level6SlotIndex.updateSlot(biter.SetBits[0], level5SlotIndex)
	//}
	//level6Slots := level5Slots >> 6
	//level6Slot := level6Slots % 64
	//shouldExpand = level6Slot == 0
	//if !shouldExpand {
	//	level6SlotIndex.children[level6Slot-1] = level5SlotIndexSeq
	//	editing.levels[5] = newSlotIndex(editing.strategy, 5)
	//	return level0Slot, level1Slot, level2Slot, level2Slots, nil
	//}
	//level6SlotIndex.children[63] = level5SlotIndexSeq
	//editing.levels[5] = newSlotIndex(editing.strategy, 5)
	//level6SlotIndexSeq := uint64(editing.tailSlotIndexSeq)
	//editing.tailSlotIndexSeq, err = editing.writeSlotIndex(editing.tailSlotIndexSeq, level6SlotIndex)
	//ctx.TraceCall("callee!editing.writeSlotIndex", err)
	//if err != nil {
	//	return 0, 0, 0, 0, err
	//}
	//justMovedTopLevel = editing.topLevel < 7
	//level7SlotIndex := editing.editLevel(7)
	//if justMovedTopLevel {
	//	level7SlotIndex.updateSlot(biter.SetBits[0], level6SlotIndex)
	//}
	//level7Slots := level6Slots >> 6
	//level7Slot := level7Slots % 64
	//shouldExpand = level7Slot == 0
	//if !shouldExpand {
	//	level7SlotIndex.children[level7Slot-1] = level6SlotIndexSeq
	//	editing.levels[6] = newSlotIndex(editing.strategy, 6)
	//	return level0Slot, level1Slot, level2Slot, level2Slots, nil
	//}
	//level7SlotIndex.children[63] = level6SlotIndexSeq
	//editing.levels[6] = newSlotIndex(editing.strategy, 6)
	//level7SlotIndexSeq := uint64(editing.tailSlotIndexSeq)
	//editing.tailSlotIndexSeq, err = editing.writeSlotIndex(editing.tailSlotIndexSeq, level7SlotIndex)
	//ctx.TraceCall("callee!editing.writeSlotIndex", err)
	//if err != nil {
	//	return 0, 0, 0, 0, err
	//}
	//justMovedTopLevel = editing.topLevel < 8
	//level8SlotIndex := editing.editLevel(8)
	//if justMovedTopLevel {
	//	level8SlotIndex.updateSlot(biter.SetBits[0], level7SlotIndex)
	//}
	//level8Slots := level7Slots >> 6
	//level8Slot := level8Slots % 64
	//shouldExpand = level8Slot == 0
	//if !shouldExpand {
	//	level8SlotIndex.children[level8Slot-1] = level7SlotIndexSeq
	//	editing.levels[7] = newSlotIndex(editing.strategy, 7)
	//	return level0Slot, level1Slot, level2Slot, level2Slots, nil
	//}
	panic("bloom filter tree exceed capacity")
}

func (editing *editingHead) editLevel(level level) (*slotIndex, error) {
	if level > editing.topLevel {
		editing.topLevel = level
		slotIndexSeq, nextSlotIndexSeq, slotIndex, err := editing.slotIndexManager.newSlotIndex(
			editing.tailSlotIndexSeq, level)
		editing.tailSlotIndexSeq = nextSlotIndexSeq
		if err != nil {
			return nil, err
		}
		editing.levels[level] = slotIndexSeq
		return slotIndex, nil
	}
	return editing.slotIndexManager.mapWritableSlotIndex(editing.levels[level], level)
}

func (segment *headSegmentVersion) scanForward(
	ctx countlog.Context, blockManager *blockManager, slotIndexManager *slotIndexManager,
	filters ...Filter) chunkIterator {
	return iterateChunks(nil)
}
