package lstore

import (
	"os"
	"github.com/esdb/lstore/ref"
	"io"
	"github.com/edsrzf/mmap-go"
	"github.com/esdb/gocodec"
	"github.com/esdb/pbloom"
	"github.com/esdb/biter"
)

type compactingSegmentVersion struct {
	SegmentHeader
	tailSeq      RowSeq
	slotIndex    slotIndex
	blocks       []BlockSeq // 64 slots
	tailBlockSeq BlockSeq
	tailSlot     biter.Slot
}

type compactingSegment struct {
	compactingSegmentVersion
	*ref.ReferenceCounted
	path string
}

func openCompactingSegment(path string) (*compactingSegment, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	resources := []io.Closer{file}
	segment := &compactingSegment{}
	readMMap, err := mmap.Map(file, mmap.COPY, 0)
	if err != nil {
		file.Close()
		return nil, err
	}
	resources = append(resources, ref.NewResource("readMMap", func() error {
		return readMMap.Unmap()
	}))
	iter := gocodec.NewIterator(readMMap)
	storage, _ := iter.Unmarshal((*compactingSegmentVersion)(nil)).(*compactingSegmentVersion)
	if iter.Error != nil {
		readMMap.Unmap()
		file.Close()
		return nil, iter.Error
	}
	segment.path = path
	segment.compactingSegmentVersion = *storage
	segment.ReferenceCounted = ref.NewReferenceCounted("compacting chunk", resources...)
	return segment, nil
}

func createCompactingSegment(path string, segment compactingSegmentVersion) (*compactingSegment, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	stream := gocodec.NewStream(nil)
	stream.Marshal(segment)
	if stream.Error != nil {
		return nil, stream.Error
	}
	_, err = file.Write(stream.Buffer())
	if err != nil {
		return nil, err
	}
	return &compactingSegment{
		path:                     path,
		compactingSegmentVersion: segment,
		ReferenceCounted:         ref.NewReferenceCounted("compacting chunk"),
	}, nil
}

func (segment *compactingSegment) scanForward(blockManager *blockManager, filters []Filter) chunkIterator {
	if segment == nil {
		return iterateChunks(nil)
	}
	result := segment.slotIndex.search(filters)
	iter := result.ScanForward()
	return func() (chunk, error) {
		slot := iter()
		if slot == biter.NotFound {
			return nil, io.EOF
		}
		blockSeq := segment.blocks[slot]
		block, err := blockManager.readBlock(blockSeq)
		if err != nil {
			return nil, err
		}
		return block, nil
	}
}

func (segment *compactingSegment) getTailSlot() biter.Slot {
	if segment == nil {
		return 0
	}
	return segment.tailSlot
}

func (segment *compactingSegment) getTailSeq() RowSeq {
	if segment == nil {
		return 0
	}
	return segment.tailSeq
}

func (segment *compactingSegment) nextSlot(
	startSeq RowSeq, indexingStrategy *indexingStrategy,
	hashingStrategy *pbloom.HashingStrategy) compactingSegmentVersion {
	if segment == nil {
		return newCompactingSegmentVersion(startSeq, indexingStrategy, hashingStrategy)
	}
	return segment.compactingSegmentVersion.nextSlot()
}

func newCompactingSegmentVersion(
	startSeq RowSeq, indexingStrategy *indexingStrategy,
	hashingStrategy *pbloom.HashingStrategy) compactingSegmentVersion {
	return compactingSegmentVersion{
		SegmentHeader: SegmentHeader{
			SegmentType: SegmentTypeCompacting,
			StartSeq:    startSeq,
		},
		slotIndex:    newSlotIndex(indexingStrategy, hashingStrategy),
		blocks:       make([]BlockSeq, 64),
		tailBlockSeq: 0,
		tailSlot:     0,
	}
}

func (version *compactingSegmentVersion) nextSlot() (compactingSegmentVersion) {
	if version == nil {
	}
	if version.tailSlot == 63 {
		panic("compacting segment can only hold 64 slots")
	}
	newVersion := compactingSegmentVersion{}
	newVersion.SegmentHeader = version.SegmentHeader
	newVersion.tailBlockSeq = version.tailBlockSeq
	newVersion.tailSlot = version.tailSlot + 1
	newVersion.blocks = append([]BlockSeq(nil), version.blocks...)
	newVersion.slotIndex = version.slotIndex.copy()
	return newVersion
}
