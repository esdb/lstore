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
	topLevel         level         // minimum 3 level
	levels           [] *slotIndex // total 9 levels
}

type headSegment struct {
	*headSegmentVersion
	*ref.ReferenceCounted
	writeMMap mmap.MMap
}

type editingHead struct {
	*headSegmentVersion
	writeBlock     func(blockSeq, *block) (blockSeq, blockSeq, error)
	writeSlotIndex func(slotIndexSeq, *slotIndex) (slotIndexSeq, error)
	strategy       *IndexingStrategy
}

func openHeadSegment(ctx countlog.Context, config *Config, strategy *IndexingStrategy) (*headSegment, error) {
	headSegmentPath := config.HeadSegmentPath()
	file, err := os.OpenFile(headSegmentPath, os.O_RDWR, 0666)
	if os.IsNotExist(err) {
		file, err = initIndexedSegment(ctx, config, strategy)
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

func initIndexedSegment(ctx countlog.Context, config *Config, strategy *IndexingStrategy) (*os.File, error) {
	levels := make([]*slotIndex, 9)
	for i := level0; i < level(len(levels)); i++ {
		levels[i] = newSlotIndex(strategy, i)
	}
	stream := gocodec.NewStream(nil)
	stream.Marshal(headSegmentVersion{
		segmentHeader: segmentHeader{segmentType: segmentTypeHead},
		topLevel:      2,
		levels:        levels,
	})
	ctx.TraceCall("callee!stream.Marshal", stream.Error)
	if stream.Error != nil {
		return nil, stream.Error
	}
	segmentPath := config.HeadSegmentPath()
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
	level0Slot, level1Slot, level2Slot, level2Slots, err := editing.nextSlot(ctx)
	level0SlotMask := biter.SetBits[level0Slot]
	level1SlotMask := biter.SetBits[level1Slot]
	level2SlotMask := biter.SetBits[level2Slot]
	if err != nil {
		return err
	}
	level0SlotIndex := editing.editLevel(level0)
	level1SlotIndex := editing.editLevel(level1)
	level2SlotIndex := editing.editLevel(level2)
	blockSeq := editing.tailBlockSeq
	// hash will update block, so call it before write
	blkHash := blk.Hash(editing.strategy)
	blockSeq, editing.tailBlockSeq, err = editing.writeBlock(blockSeq, blk)
	ctx.TraceCall("callee!editing.writeBlock", err)
	if err != nil {
		return err
	}
	level0SlotIndex.children[level0Slot] = uint64(blockSeq)
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
			levelNSlots := level2Slots
			for j := level(3); j <= editing.topLevel; j++ {
				levelNSlots = levelNSlots >> 6
				parentPbf := editing.levels[j].pbfs[i]
				levelNMask := biter.SetBits[levelNSlots%64]
				for _, location := range locations {
					parentPbf[location] |= levelNMask
				}
			}
		}
	}
	editing.tailOffset += Offset(blockLength)
	return nil
}

func (editing *editingHead) nextSlot(ctx countlog.Context) (int, int, int, int, error) {
	level0SlotIndex := editing.editLevel(level0)
	level0Slots := int(editing.tailOffset) >> blockLengthInPowerOfTwo
	level0Slot := level0Slots % 64
	level1Slots := level0Slots >> 6
	level1Slot := level1Slots % 64
	level2Slots := level1Slots >> 6
	level2Slot := level2Slots % 64
	shouldExpand := level0Slot == 0 && level0Slots > 0
	if !shouldExpand {
		return level0Slot, level1Slot, level2Slot, level2Slots, nil
	}
	var err error
	level0SlotIndexSeq := uint64(editing.tailSlotIndexSeq)
	editing.tailSlotIndexSeq, err = editing.writeSlotIndex(editing.tailSlotIndexSeq, level0SlotIndex)
	ctx.TraceCall("callee!editing.writeSlotIndex", err)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	level1SlotIndex := editing.editLevel(level1)
	shouldExpand = level1Slot == 0
	if !shouldExpand {
		level1SlotIndex.children[level1Slot-1] = level0SlotIndexSeq
		editing.levels[level0] = newSlotIndex(editing.strategy, level0)
		return level0Slot, level1Slot, level2Slot, level2Slots, nil
	}
	level1SlotIndex.children[63] = level0SlotIndexSeq
	editing.levels[level0] = newSlotIndex(editing.strategy, level0)
	level1SlotIndexSeq := uint64(editing.tailSlotIndexSeq)
	editing.tailSlotIndexSeq, err = editing.writeSlotIndex(editing.tailSlotIndexSeq, level1SlotIndex)
	ctx.TraceCall("callee!editing.writeSlotIndex", err)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	level2SlotIndex := editing.editLevel(level2)
	shouldExpand = level2Slot == 0
	if !shouldExpand {
		level2SlotIndex.children[level2Slot-1] = level1SlotIndexSeq
		editing.levels[level1] = newSlotIndex(editing.strategy, level1)
		return level0Slot, level1Slot, level2Slot, level2Slots, nil
	}
	level2SlotIndex.children[63] = level1SlotIndexSeq
	editing.levels[level1] = newSlotIndex(editing.strategy, level1)
	level2SlotIndexSeq := uint64(editing.tailSlotIndexSeq)
	editing.tailSlotIndexSeq, err = editing.writeSlotIndex(editing.tailSlotIndexSeq, level2SlotIndex)
	ctx.TraceCall("callee!editing.writeSlotIndex", err)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	justMovedTopLevel := editing.topLevel < 3
	level3SlotIndex := editing.editLevel(3)
	if justMovedTopLevel {
		level3SlotIndex.updateSlot(biter.SetBits[0], level2SlotIndex)
	}
	level3Slots := level2Slots >> 6
	level3Slot := level3Slots % 64
	shouldExpand = level3Slot == 0
	if !shouldExpand {
		level3SlotIndex.children[level3Slot-1] = level2SlotIndexSeq
		editing.levels[level2] = newSlotIndex(editing.strategy, level2)
		return level0Slot, level1Slot, level2Slot, level2Slots, nil
	}
	level3SlotIndex.children[63] = level2SlotIndexSeq
	editing.levels[level2] = newSlotIndex(editing.strategy, level2)
	level3SlotIndexSeq := uint64(editing.tailSlotIndexSeq)
	editing.tailSlotIndexSeq, err = editing.writeSlotIndex(editing.tailSlotIndexSeq, level3SlotIndex)
	ctx.TraceCall("callee!editing.writeSlotIndex", err)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	justMovedTopLevel = editing.topLevel < 4
	level4SlotIndex := editing.editLevel(4)
	if justMovedTopLevel {
		level4SlotIndex.updateSlot(biter.SetBits[0], level3SlotIndex)
	}
	level4Slots := level3Slots >> 6
	level4Slot := level4Slots % 64
	shouldExpand = level4Slot == 0
	if !shouldExpand {
		level4SlotIndex.children[level4Slot-1] = level3SlotIndexSeq
		editing.levels[3] = newSlotIndex(editing.strategy, 3)
		return level0Slot, level1Slot, level2Slot, level2Slots, nil
	}
	level4SlotIndex.children[63] = level3SlotIndexSeq
	editing.levels[3] = newSlotIndex(editing.strategy, 3)
	level4SlotIndexSeq := uint64(editing.tailSlotIndexSeq)
	editing.tailSlotIndexSeq, err = editing.writeSlotIndex(editing.tailSlotIndexSeq, level4SlotIndex)
	ctx.TraceCall("callee!editing.writeSlotIndex", err)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	justMovedTopLevel = editing.topLevel < 5
	level5SlotIndex := editing.editLevel(5)
	if justMovedTopLevel {
		level5SlotIndex.updateSlot(biter.SetBits[0], level4SlotIndex)
	}
	level5Slots := level4Slots >> 6
	level5Slot := level5Slots % 64
	shouldExpand = level5Slot == 0
	if !shouldExpand {
		level5SlotIndex.children[level5Slot-1] = level4SlotIndexSeq
		editing.levels[4] = newSlotIndex(editing.strategy, 4)
		return level0Slot, level1Slot, level2Slot, level2Slots, nil
	}
	level5SlotIndex.children[63] = level4SlotIndexSeq
	editing.levels[4] = newSlotIndex(editing.strategy, 4)
	level5SlotIndexSeq := uint64(editing.tailSlotIndexSeq)
	editing.tailSlotIndexSeq, err = editing.writeSlotIndex(editing.tailSlotIndexSeq, level5SlotIndex)
	ctx.TraceCall("callee!editing.writeSlotIndex", err)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	justMovedTopLevel = editing.topLevel < 6
	level6SlotIndex := editing.editLevel(6)
	if justMovedTopLevel {
		level6SlotIndex.updateSlot(biter.SetBits[0], level5SlotIndex)
	}
	level6Slots := level5Slots >> 6
	level6Slot := level6Slots % 64
	shouldExpand = level6Slot == 0
	if !shouldExpand {
		level6SlotIndex.children[level6Slot-1] = level5SlotIndexSeq
		editing.levels[5] = newSlotIndex(editing.strategy, 5)
		return level0Slot, level1Slot, level2Slot, level2Slots, nil
	}
	level6SlotIndex.children[63] = level5SlotIndexSeq
	editing.levels[5] = newSlotIndex(editing.strategy, 5)
	level6SlotIndexSeq := uint64(editing.tailSlotIndexSeq)
	editing.tailSlotIndexSeq, err = editing.writeSlotIndex(editing.tailSlotIndexSeq, level6SlotIndex)
	ctx.TraceCall("callee!editing.writeSlotIndex", err)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	justMovedTopLevel = editing.topLevel < 7
	level7SlotIndex := editing.editLevel(7)
	if justMovedTopLevel {
		level7SlotIndex.updateSlot(biter.SetBits[0], level6SlotIndex)
	}
	level7Slots := level6Slots >> 6
	level7Slot := level7Slots % 64
	shouldExpand = level7Slot == 0
	if !shouldExpand {
		level7SlotIndex.children[level7Slot-1] = level6SlotIndexSeq
		editing.levels[6] = newSlotIndex(editing.strategy, 6)
		return level0Slot, level1Slot, level2Slot, level2Slots, nil
	}
	level7SlotIndex.children[63] = level6SlotIndexSeq
	editing.levels[6] = newSlotIndex(editing.strategy, 6)
	level7SlotIndexSeq := uint64(editing.tailSlotIndexSeq)
	editing.tailSlotIndexSeq, err = editing.writeSlotIndex(editing.tailSlotIndexSeq, level7SlotIndex)
	ctx.TraceCall("callee!editing.writeSlotIndex", err)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	justMovedTopLevel = editing.topLevel < 8
	level8SlotIndex := editing.editLevel(8)
	if justMovedTopLevel {
		level8SlotIndex.updateSlot(biter.SetBits[0], level7SlotIndex)
	}
	level8Slots := level7Slots >> 6
	level8Slot := level8Slots % 64
	shouldExpand = level8Slot == 0
	if !shouldExpand {
		level8SlotIndex.children[level8Slot-1] = level7SlotIndexSeq
		editing.levels[7] = newSlotIndex(editing.strategy, 7)
		return level0Slot, level1Slot, level2Slot, level2Slots, nil
	}
	panic("bloom filter tree exceed capacity")
	return level0Slot, level1Slot, level2Slot, level2Slots, nil
}

func (editing *editingHead) editLevel(level level) *slotIndex {
	if level > editing.topLevel {
		editing.topLevel = level
	}
	return editing.levels[level]
}

func (segment *headSegmentVersion) scanForward(
	ctx countlog.Context, blockManager *blockManager, slotIndexManager *slotIndexManager,
	filters ...Filter) chunkIterator {
	var iters []func() biter.Slot
	for level := segment.topLevel; level >= 0; level-- {
		result := segment.levels[level].search(level, filters...)
		if result == 0 {
			return iterateChunks(nil)
		}
		iter := result.ScanForward()
		iters = append(iters, iter)
	}
	return func() (chunk, error) {
		level := len(iters) - 1
		slot := iters[level]()
		blkSeq := blockSeq(segment.levels[level].children[slot])
		blk, err := blockManager.readBlock(blkSeq)
		if err != nil {
			return nil, err
		}
		return blk, nil
	}
}
