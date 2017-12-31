package lstore

import (
	"os"
	"github.com/edsrzf/mmap-go"
	"github.com/v2pro/plz/countlog"
	"github.com/esdb/gocodec"
	"errors"
	"sync/atomic"
	"io"
	"github.com/esdb/lstore/ref"
	"path"
)

var SegmentOverflowError = errors.New("please rotate to new chunk")

type TailSegment struct {
	segmentHeader
	*ref.ReferenceCounted
	path     string
	readBuf  []byte
	file     *os.File
	readMMap mmap.MMap
	tail     uint64 // offset, writer use atomic to notify readers
}

func openTailSegment(path string, maxSize int64, startOffset Offset) (*TailSegment, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0666)
	if os.IsNotExist(err) {
		file, err = createTailSegment(path, maxSize, startOffset)
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	resources := []io.Closer{file}
	segment := &TailSegment{}
	segment.file = file
	readMMap, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		file.Close()
		countlog.Error("event!chunk.failed to mmap as RDONLY", "err", err, "path", path)
		return nil, err
	}
	resources = append(resources, ref.NewResource("tail segment readMMap", func() error {
		return readMMap.Unmap()
	}))
	iter := gocodec.NewIterator(readMMap)
	segmentHeader, _ := iter.Copy((*segmentHeader)(nil)).(*segmentHeader)
	if iter.Error != nil {
		readMMap.Unmap()
		file.Close()
		return nil, iter.Error
	}
	segment.segmentHeader = *segmentHeader
	segment.readBuf = iter.Buffer()
	segment.path = path
	segment.ReferenceCounted = ref.NewReferenceCounted("tail segment", resources...)
	return segment, nil
}

func createTailSegment(filename string, maxSize int64, startOffset Offset) (*os.File, error) {
	os.MkdirAll(path.Dir(filename), 0777)
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	err = file.Truncate(maxSize)
	if err != nil {
		return nil, err
	}
	stream := gocodec.NewStream(nil)
	stream.Marshal(segmentHeader{segmentType: SegmentTypeRowBased, startOffset: startOffset})
	if stream.Error != nil {
		return nil, stream.Error
	}
	_, err = file.Write(stream.Buffer())
	if err != nil {
		return nil, err
	}
	return os.OpenFile(filename, os.O_RDWR, 0666)
}

func (segment *TailSegment) updateTail(tail Offset) {
	atomic.StoreUint64(&segment.tail, uint64(tail))
}

func (segment *TailSegment) getTail() Offset {
	return Offset(atomic.LoadUint64(&segment.tail))
}

func (segment *TailSegment) read(reader *Reader) (bool, error) {
	iter := reader.gocIter
	if reader.tailOffset == segment.getTail() {
		// tail not moved
		return false, nil
	}
	buf := segment.readBuf[reader.tailSeq:]
	bufSize := uint64(len(buf))
	iter.Reset(buf)
	newRowsCount := 0
	startOffset := reader.currentVersion.tailSegment.startOffset
	for {
		currentSeq := reader.tailSeq + (bufSize - uint64(len(iter.Buffer())))
		entry, _ := iter.Copy((*Entry)(nil)).(*Entry)
		if iter.Error == io.EOF {
			reader.tailSeq = currentSeq
			return true, nil
		}
		if iter.Error != nil {
			return false, iter.Error
		}
		offset := startOffset + Offset(len(reader.tailRows))
		reader.tailRows = append(reader.tailRows, Row{Entry: entry, Offset: offset})
		reader.tailOffset = offset + 1
		newRowsCount++
	}
}
