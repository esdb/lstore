package lstore

import (
	"os"
	"github.com/edsrzf/mmap-go"
	"github.com/v2pro/plz/countlog"
	"github.com/esdb/gocodec"
	"errors"
	"sync/atomic"
	"io"
)

var SegmentOverflowError = errors.New("please rotate to new segment")
type TailSegment struct {
	SegmentHeader
	Path      string
	readBuf   []byte
	resources []func() error
	tail      uint64 // writer use atomic to notify readers
}

func openTailSegment(path string, maxSize int64, startOffset Offset) (*TailSegment, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		file, err = createTailSegment(path, maxSize, startOffset)
		if err != nil {
			return nil, err
		}
	}
	segment := &TailSegment{}
	segment.resources = append(segment.resources, func() error {
		return file.Close()
	})
	writeMMap, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		countlog.Error("event!segment.failed to mmap as RDWR", "err", err, "path", path)
		return nil, err
	}
	segment.resources = append(segment.resources, func() error {
		return writeMMap.Unmap()
	})
	segment.writeMMap = writeMMap
	iter := gocodec.NewIterator(writeMMap)
	headerBytes := append([]byte(nil), iter.Skip()...)
	segment.writeBuf = iter.Buffer()
	iter.Reset(headerBytes)
	segmentHeader, _ := iter.Unmarshal((*SegmentHeader)(nil)).(*SegmentHeader)
	if iter.Error != nil {
		return nil, iter.Error
	}
	segment.SegmentHeader = *segmentHeader
	readMMap, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		countlog.Error("event!segment.failed to mmap as RDONLY", "err", err, "path", path)
		return nil, err
	}
	segment.resources = append(segment.resources, func() error {
		return readMMap.Unmap()
	})
	iter.Reset(readMMap)
	iter.Skip()
	if iter.Error != nil {
		return nil, iter.Error
	}
	segment.SegmentHeader = *segmentHeader
	segment.readBuf = iter.Buffer()
	segment.Path = path
	segment.tail = uint64(startOffset)
	return segment, nil
}

func createTailSegment(filename string, maxSize int64, startOffset Offset) (*os.File, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	err = file.Truncate(maxSize)
	if err != nil {
		return nil, err
	}
	stream := gocodec.NewStream(nil)
	stream.Marshal(SegmentHeader{SegmentType: SegmentTypeRowBased, StartOffset: startOffset})
	if stream.Error != nil {
		return nil, stream.Error
	}
	_, err = file.Write(stream.Buffer())
	if err != nil {
		return nil, err
	}
	err = file.Close()
	if err != nil {
		return nil, err
	}
	return os.OpenFile(filename, os.O_RDWR, 0666)
}

func (segment *TailSegment) Close() error {
	failed := false
	for _, resource := range segment.resources {
		err := resource()
		if err != nil {
			countlog.Error("event!segment.failed to close resource", "err", err)
			failed = true
		}
	}
	if failed {
		return errors.New("not all resources closed properly")
	}
	return nil
}

func (segment *TailSegment) updateTail(tail Offset) {
	atomic.StoreUint64(&segment.tail, uint64(tail))
}

func (segment *TailSegment) getTail() Offset {
	return Offset(atomic.LoadUint64(&segment.tail))
}

func (segment *TailSegment) read(reader *Reader) error {
	iter := reader.gocIter
	var rows []Row
	startOffset := reader.tailOffset
	if startOffset == segment.getTail() {
		// tail not moved
		return nil
	}
	buf := segment.readBuf[startOffset:]
	bufSize := Offset(len(buf))
	iter.Reset(buf)
	for {
		currentOffset := startOffset + (bufSize - Offset(len(iter.Buffer())))
		entry, _ := iter.Copy((*Entry)(nil)).(*Entry)
		if iter.Error == io.EOF {
			reader.tailRows = rows
			reader.tailOffset = currentOffset
			reader.tailBlock = &rowBasedBlock{rows: reader.tailRows}
			return nil
		}
		if iter.Error != nil {
			return iter.Error
		}
		rows = append(rows, Row{Entry: entry, Offset: currentOffset})
	}
}
