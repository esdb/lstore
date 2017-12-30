package lstore

import (
	"os"
	"github.com/esdb/lstore/ref"
	"io"
	"github.com/edsrzf/mmap-go"
	"github.com/esdb/gocodec"
	"github.com/esdb/biter"
	"fmt"
	"github.com/v2pro/plz/countlog"
)

type indexedSegmentVersion struct {
	segmentHeader
	slotIndex slotIndex
	children  []blockSeq // 64 slots
	tailSeq   RowSeq
	tailSlot  biter.Slot
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
	tailBlockSeq blockSeq // next block write to this seq
}

type rootIndexedSegment struct {
	rootIndexedSegmentVersion
	*ref.ReferenceCounted
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
	segment.rootIndexedSegmentVersion = *version
	segment.children = make([]*indexedSegment, 64)
	for i := biter.Slot(0); i <= segment.tailSlot; i++ {
		childPath := fmt.Sprintf("%s.%d", path, i)
		child, err := openIndexedSegment(childPath)
		if err != nil {
			countlog.Error("event!indexedSegment.failed to load child",
				"childPath", childPath, "err", err)
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
		countlog.Error("event!rootIndexedSegment.failed to create", "err", err, "path", path)
		return nil, err
	}
	countlog.Trace("event!rootIndexedSegment.create",
		"path", path, "startSeq", segment.startSeq, "tailSeq", segment.tailSeq,
		"tailSlot", segment.tailSlot, "tailBlockSeq", segment.tailBlockSeq)
	var resources []io.Closer
	for _, child := range children {
		if child == nil {
			continue
		}
		if !child.Acquire() {
			panic("acquire reference counter should not fail during version rotation")
		}
		resources = append(resources, child)
	}
	return &rootIndexedSegment{
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
		countlog.Error("event!indexedSegment.failed to create", "err", err, "path", path)
		return nil, err
	}
	countlog.Trace("event!indexedSegment.create",
		"path", path, "startSeq", segment.startSeq, "tailSeq", segment.tailSeq,
			"tailSlot", segment.tailSlot)
	return &indexedSegment{
		indexedSegmentVersion: segment,
		ReferenceCounted:      ref.NewReferenceCounted("indexed segment"),
	}, nil
}

func newRootIndexedSegmentVersion(startSeq RowSeq, strategy *indexingStrategy) rootIndexedSegmentVersion {
	return rootIndexedSegmentVersion{
		segmentHeader: segmentHeader{
			segmentType: SegmentTypeRootIndexed,
			startSeq:    startSeq,
		},
		slotIndex: newSlotIndex(strategy, strategy.bigHashingStrategy),
	}
}

func newIndexedSegmentVersion(startSeq RowSeq, strategy *indexingStrategy) indexedSegmentVersion {
	return indexedSegmentVersion{
		segmentHeader: segmentHeader{
			segmentType: SegmentTypeIndexed,
			startSeq:    startSeq,
		},
		slotIndex: newSlotIndex(strategy, strategy.smallHashingStrategy),
		children:  make([]blockSeq, 64),
	}
}

func (segment *rootIndexedSegment) scanForward(blockManager *blockManager, filters []Filter) chunkIterator {
	if segment == nil {
		return iterateChunks(nil)
	}
	result := segment.slotIndex.searchBig(filters)
	iter := result.ScanForward()
	slot := iter()
	var childIter func() (chunk, error)
	return func() (chunk, error) {
		for {
			if slot == biter.NotFound {
				return nil, io.EOF
			}
			if childIter == nil {
				childIter = segment.children[slot].scanForward(blockManager, filters)
			}
			chunk, err := childIter()
			if err == io.EOF {
				childIter = nil
				slot = iter()
				continue
			}
			return chunk, err
		}
	}
}

func (segment *indexedSegment) scanForward(blockManager *blockManager, filters []Filter) chunkIterator {
	result := segment.slotIndex.searchSmall(filters)
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

func (version *rootIndexedSegmentVersion) isFull() bool {
	return version.tailSlot >= 63
}

func (version *indexedSegmentVersion) isFull() bool {
	return version.tailSlot >= 63
}

func (segment *rootIndexedSegment) getChildren() []*indexedSegment {
	if segment == nil {
		return make([]*indexedSegment, 64)
	}
	return segment.children
}

func (segment *rootIndexedSegment) currentSlot(
	startSeq RowSeq, strategy *indexingStrategy) (rootIndexedSegmentVersion, indexedSegmentVersion) {
	if segment == nil {
		newVersion := newRootIndexedSegmentVersion(startSeq, strategy)
		newVersion.tailSlot = 0
		return newVersion, newIndexedSegmentVersion(startSeq, strategy)
	}
	child := segment.children[segment.tailSlot]
	return segment.rootIndexedSegmentVersion, child.indexedSegmentVersion
}

func (version *rootIndexedSegmentVersion) nextSlot(
	startSeq RowSeq, strategy *indexingStrategy, child indexedSegmentVersion) (
	rootIndexedSegmentVersion, indexedSegmentVersion) {
	newVersion := rootIndexedSegmentVersion{}
	newVersion.segmentHeader = version.segmentHeader
	newVersion.tailBlockSeq = version.tailBlockSeq
	newVersion.tailSeq = version.tailSeq
	newVersion.slotIndex = version.slotIndex.copy()
	if child.isFull() {
		newVersion.tailSlot = version.tailSlot + 1
		return newVersion, newIndexedSegmentVersion(startSeq, strategy)
	}
	newVersion.tailSlot = version.tailSlot
	return newVersion, child.nextSlot()
}

func (version *indexedSegmentVersion) nextSlot() (indexedSegmentVersion) {
	newVersion := indexedSegmentVersion{}
	newVersion.segmentHeader = version.segmentHeader
	newVersion.tailSlot = version.tailSlot + 1
	newVersion.children = append([]blockSeq(nil), version.children...)
	newVersion.slotIndex = version.slotIndex.copy()
	return newVersion
}
