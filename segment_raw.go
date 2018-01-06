package lstore

import (
	"github.com/edsrzf/mmap-go"
	"github.com/v2pro/plz/countlog"
	"github.com/esdb/gocodec"
	"os"
	"io"
	"github.com/esdb/lstore/ref"
	"fmt"
	"github.com/v2pro/plz"
	"strconv"
	"github.com/esdb/pbloom"
	"unsafe"
	"github.com/esdb/biter"
)

type rawSegment struct {
	segmentHeader
	*ref.ReferenceCounted
	rows []*Entry
}

func openRawSegment(ctx countlog.Context, path string) (*rawSegment, error) {
	file, err := os.OpenFile(path, os.O_RDONLY, 0666)
	ctx.TraceCall("callee!os.OpenFile", err)
	if err != nil {
		return nil, err
	}
	segment := &rawSegment{}
	readMMap, err := mmap.Map(file, mmap.COPY, 0)
	if err != nil {
		plz.Close(file)
		countlog.Error("event!raw.failed to mmap as COPY", "err", err, "path", path)
		return nil, err
	}
	plz.Close(file)
	var resources []io.Closer
	resources = append(resources, plz.WrapCloser(readMMap.Unmap))
	iter := gocodec.NewIterator(readMMap)
	segmentHeader, _ := iter.Unmarshal((*segmentHeader)(nil)).(*segmentHeader)
	if iter.Error != nil {
		countlog.Error("event!raw.failed to unmarshal header", "err", iter.Error, "path", path)
		return nil, iter.Error
	}
	segment.segmentHeader = *segmentHeader
	segment.rows, err = segment.loadRows(ctx, iter)
	if err != nil {
		countlog.Error("event!raw.failed to unmarshal rows", "err", iter.Error, "path", path)
		return nil, err
	}
	segment.ReferenceCounted = ref.NewReferenceCounted(fmt.Sprintf("raw segment@%d", segment.startOffset), resources...)
	return segment, nil
}

func (segment *rawSegment) loadRows(ctx countlog.Context, iter *gocodec.Iterator) ([]*Entry, error) {
	var rows []*Entry
	for {
		iter.Reset(iter.Buffer())
		entry, _ := iter.Unmarshal((*Entry)(nil)).(*Entry)
		if iter.Error == io.EOF {
			return rows, nil
		}
		ctx.TraceCall("callee!iter.Unmarshal", iter.Error)
		if iter.Error != nil {
			return nil, fmt.Errorf("load raw segment rows failed: %v", iter.Error.Error())
		}
		rows = append(rows, entry)
	}
}

func (segment *rawSegment) searchForward(ctx countlog.Context, startOffset Offset, tailOffset Offset,
	filter Filter, cb SearchCallback) error {
	for i, entry := range segment.rows {
		offset := segment.startOffset + Offset(i)
		if offset < startOffset || offset >= tailOffset {
			continue
		}
		if filter.matchesEntry(entry) {
			if err := cb.HandleRow(offset, entry); err != nil {
				return err
			}
		}
	}
	return nil
}

func (segment *rawSegment) String() string {
	if len(segment.rows) == 0 {
		return "rawSegment{}"
	}
	start := int(segment.startOffset)
	end := start + len(segment.rows) - 1
	desc := "rawSegment{" + strconv.Itoa(start) + "~" + strconv.Itoa(end) + "}"
	return desc
}


type rawSegmentIndexRoot struct {
	pbfs     []pbloom.ParallelBloomFilter // 64 slots
	tailSlot biter.Slot
}

type rawSegmentIndexChild struct {
	pbfs     []pbloom.ParallelBloomFilter // 64 slots
	children []*Entry
	tailSlot biter.Slot
}

// rawSegmentIndex only resides in memory
type rawSegmentIndex struct {
	startOffset Offset
	tailOffset  Offset
	root        *rawSegmentIndexRoot
	children    []*rawSegmentIndexChild
	strategy    *IndexingStrategy
}

func newRawSegmentIndex(strategy *IndexingStrategy, startOffset Offset) *rawSegmentIndex {
	children := make([]*rawSegmentIndexChild, 65)
	for i := 0; i < 65; i++ {
		hashingStrategy := strategy.tinyHashingStrategy
		pbfs := make([]pbloom.ParallelBloomFilter, strategy.bloomFilterIndexedColumnsCount())
		for i := 0; i < len(pbfs); i++ {
			pbfs[i] = hashingStrategy.New()
		}
		children[i] = &rawSegmentIndexChild{pbfs, make([]*Entry, 64), 0}
	}
	hashingStrategy := strategy.tinyHashingStrategy
	pbfs := make([]pbloom.ParallelBloomFilter, strategy.bloomFilterIndexedColumnsCount())
	for i := 0; i < len(pbfs); i++ {
		pbfs[i] = hashingStrategy.New()
	}
	return &rawSegmentIndex{
		strategy:    strategy,
		startOffset: startOffset,
		tailOffset:  startOffset,
		children:    children,
		root:        &rawSegmentIndexRoot{pbfs, 0},
	}
}

func (index *rawSegmentIndex) add(entry *Entry) {
	root := index.root
	rootTail := root.tailSlot
	child := index.children[rootTail]
	strategy := index.strategy
	childTail := child.tailSlot
	for _, bfIndexedColumn := range strategy.bloomFilterIndexedBlobColumns {
		indexedColumn := bfIndexedColumn.IndexedColumn()
		sourceColumn := bfIndexedColumn.SourceColumn()
		sourceValue := entry.BlobValues[sourceColumn]
		asSlice := *(*[]byte)(unsafe.Pointer(&sourceValue))
		bloom := strategy.tinyHashingStrategy.Hash(asSlice)
		root.pbfs[indexedColumn].Put(biter.SetBits[rootTail], bloom)
		child.pbfs[indexedColumn].Put(biter.SetBits[childTail], bloom)
		child.children[childTail] = entry
	}
	index.tailOffset += 1
	if child.tailSlot == 63 {
		root.tailSlot += 1
	}
	child.tailSlot += 1
}

func (index *rawSegmentIndex) searchForward(ctx countlog.Context, startOffset Offset,
	filter Filter, cb SearchCallback) {
	root := index.root
	rootResult := filter.searchTinyIndex(root.pbfs)
	rootResult &= biter.SetBitsForwardUntil[root.tailSlot+1]
	rootIter := rootResult.ScanForward()
	for {
		rootSlot := rootIter()
		if rootSlot == biter.NotFound {
			return
		}
		child := index.children[rootSlot]
		childResult := filter.searchTinyIndex(child.pbfs)
		childResult &= biter.SetBitsForwardUntil[child.tailSlot]
		childIter := childResult.ScanForward()
		baseOffset := index.startOffset + (Offset(rootSlot) << 6) // * 64
		for {
			childSlot := childIter()
			if childSlot == biter.NotFound {
				break
			}
			entry := child.children[childSlot]
			if filter.matchesEntry(entry) {
				cb.HandleRow(baseOffset+Offset(childSlot), entry)
			}
		}
	}
}
