package lstore

import (
	"os"
	"github.com/esdb/lstore/ref"
	"io"
	"github.com/edsrzf/mmap-go"
	"github.com/esdb/gocodec"
	"github.com/esdb/pbloom"
	"github.com/esdb/biter"
	"fmt"
)

type indexedSegmentVersion struct {
	segmentHeader
	tailSeq   RowSeq
	slotIndex slotIndex
	tailSlot  biter.Slot
	children  []BlockSeq // 64 slots
}

type indexedSegment struct {
	indexedSegmentVersion
	*ref.ReferenceCounted
}

type rootIndexedSegmentVersion struct {
	segmentHeader
	tailSeq      RowSeq
	slotIndex    slotIndex
	tailSlot     biter.Slot
	tailBlockSeq BlockSeq // next block write to this seq
}

type rootIndexedSegment struct {
	rootIndexedSegmentVersion
	*ref.ReferenceCounted
	path     string
	children []*indexedSegment // 64 slots
}

func openRootIndexedSegment(path string) (*rootIndexedSegment, error) {
	file, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}
	resources := []io.Closer{file}
	segment := &rootIndexedSegment{}
	readMMap, err := mmap.Map(file, mmap.COPY, 0)
	if err != nil {
		file.Close()
		return nil, err
	}
	resources = append(resources, ref.NewResource("readMMap", func() error {
		return readMMap.Unmap()
	}))
	iter := gocodec.NewIterator(readMMap)
	version, _ := iter.Unmarshal((*rootIndexedSegmentVersion)(nil)).(*rootIndexedSegmentVersion)
	if iter.Error != nil {
		readMMap.Unmap()
		file.Close()
		return nil, iter.Error
	}
	segment.path = path
	segment.rootIndexedSegmentVersion = *version
	segment.children = make([]*indexedSegment, 64)
	for i := 0; i < 64; i++ {
		child, err := openIndexedSegment(fmt.Sprintf("%s.%d", path, i))
		if err != nil {
			return nil, err
		}
		segment.children[i] = child
		resources = append(resources, child)
	}
	segment.ReferenceCounted = ref.NewReferenceCounted("root indexed segment", resources...)
	return segment, nil
}

func openIndexedSegment(path string) (*indexedSegment, error) {
	file, err := os.OpenFile(path, os.O_RDONLY, 0666)
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
	version, _ := iter.Unmarshal((*indexedSegmentVersion)(nil)).(*indexedSegmentVersion)
	if iter.Error != nil {
		readMMap.Unmap()
		file.Close()
		return nil, iter.Error
	}
	segment.indexedSegmentVersion = *version
	segment.ReferenceCounted = ref.NewReferenceCounted("indexed segment", resources...)
	return segment, nil
}

func createRootIndexedSegment(path string, segment rootIndexedSegmentVersion, children []*indexedSegment) (*rootIndexedSegment, error) {
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
	var resources []io.Closer
	for _, child := range children {
		resources = append(resources, child)
	}
	return &rootIndexedSegment{
		path:                      path,
		rootIndexedSegmentVersion: segment,
		children:                  children,
		ReferenceCounted:          ref.NewReferenceCounted("root indexed segment", resources...),
	}, nil
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
		indexedSegmentVersion: segment,
		ReferenceCounted:      ref.NewReferenceCounted("indexed segment"),
	}, nil
}

func newRootIndexedSegmentVersion(
	startSeq RowSeq, indexingStrategy *indexingStrategy,
	hashingStrategy *pbloom.HashingStrategy) rootIndexedSegmentVersion {
	return rootIndexedSegmentVersion{
		segmentHeader: segmentHeader{
			segmentType: SegmentTypeRootIndexed,
			startSeq:    startSeq,
		},
		slotIndex:    newSlotIndex(indexingStrategy, hashingStrategy),
	}
}

func newIndexedSegmentVersion(
	startSeq RowSeq, indexingStrategy *indexingStrategy,
	hashingStrategy *pbloom.HashingStrategy) indexedSegmentVersion {
	return indexedSegmentVersion{
		segmentHeader: segmentHeader{
			segmentType: SegmentTypeIndexed,
			startSeq:    startSeq,
		},
		slotIndex:    newSlotIndex(indexingStrategy, hashingStrategy),
		children:     make([]BlockSeq, 64),
	}
}

func (segment *rootIndexedSegment) scanForward(blockManager *blockManager, filters []Filter) chunkIterator {
	panic("not implemented")
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
		blockSeq := segment.children[slot]
		block, err := blockManager.readBlock(blockSeq)
		if err != nil {
			return nil, err
		}
		return block, nil
	}
}

func (segment *rootIndexedSegment) nextSlot() (rootIndexedSegmentVersion, indexedSegmentVersion) {
	newVersion := rootIndexedSegmentVersion{}
	newVersion.segmentHeader = segment.segmentHeader
	newVersion.tailSlot = segment.tailSlot + 1
	newVersion.slotIndex = segment.slotIndex.copy()
	return newVersion, segment.children[newVersion.tailSlot].nextSlot()
}

func (version *indexedSegmentVersion) nextSlot() (indexedSegmentVersion) {
	newVersion := indexedSegmentVersion{}
	newVersion.segmentHeader = version.segmentHeader
	newVersion.tailSlot = version.tailSlot + 1
	newVersion.children = append([]BlockSeq(nil), version.children...)
	newVersion.slotIndex = version.slotIndex.copy()
	return newVersion
}
