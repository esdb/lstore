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

type indexedSegmentVersion struct {
	SegmentHeader
	tailSeq      RowSeq
	slotIndex    slotIndex
	blocks       []BlockSeq // 64 slots
	tailBlockSeq BlockSeq
	tailSlot     biter.Slot
}

type indexedSegment struct {
	indexedSegmentVersion
	*ref.ReferenceCounted
	path string
}

func openIndexedSegment(path string) (*indexedSegment, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	resources := []io.Closer{file}
	segment := &indexedSegment{}
	readMMap, err := mmap.Map(file, mmap.COPY, 0)
	if err != nil {
		file.Close()
		return nil, err
	}
	resources = append(resources, ref.NewResource("readMMap", func() error {
		return readMMap.Unmap()
	}))
	iter := gocodec.NewIterator(readMMap)
	storage, _ := iter.Unmarshal((*indexedSegmentVersion)(nil)).(*indexedSegmentVersion)
	if iter.Error != nil {
		readMMap.Unmap()
		file.Close()
		return nil, iter.Error
	}
	segment.path = path
	segment.indexedSegmentVersion = *storage
	segment.ReferenceCounted = ref.NewReferenceCounted("indexed segment", resources...)
	return segment, nil
}

func createIndexedSegment(path string, segment indexedSegmentVersion) (*indexedSegment, error) {
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
	return &indexedSegment{
		path:                  path,
		indexedSegmentVersion: segment,
		ReferenceCounted:      ref.NewReferenceCounted("indexed segment"),
	}, nil
}

func (segment *indexedSegment) scanForward(blockManager *blockManager, filters []Filter) chunkIterator {
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

func (segment *indexedSegment) getTailSlot() biter.Slot {
	if segment == nil {
		return 0
	}
	return segment.tailSlot
}

func (segment *indexedSegment) getTailSeq() RowSeq {
	if segment == nil {
		return 0
	}
	return segment.tailSeq
}

func (segment *indexedSegment) nextSlot(
	startSeq RowSeq, indexingStrategy *indexingStrategy,
	hashingStrategy *pbloom.HashingStrategy) indexedSegmentVersion {
	if segment == nil {
		return newIndexedSegmentVersion(startSeq, indexingStrategy, hashingStrategy)
	}
	return segment.indexedSegmentVersion.nextSlot()
}

func newIndexedSegmentVersion(
	startSeq RowSeq, indexingStrategy *indexingStrategy,
	hashingStrategy *pbloom.HashingStrategy) indexedSegmentVersion {
	return indexedSegmentVersion{
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

func (version *indexedSegmentVersion) nextSlot() (indexedSegmentVersion) {
	if version == nil {
	}
	if version.tailSlot == 63 {
		panic("indexed segment can only hold 64 slots")
	}
	newVersion := indexedSegmentVersion{}
	newVersion.SegmentHeader = version.SegmentHeader
	newVersion.tailBlockSeq = version.tailBlockSeq
	newVersion.tailSlot = version.tailSlot + 1
	newVersion.blocks = append([]BlockSeq(nil), version.blocks...)
	newVersion.slotIndex = version.slotIndex.copy()
	return newVersion
}
