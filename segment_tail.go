package lstore

import (
	"os"
	"github.com/edsrzf/mmap-go"
	"github.com/v2pro/plz/countlog"
	"github.com/esdb/gocodec"
	"errors"
	"sync/atomic"
	"io"
	"fmt"
)

var SegmentOverflowError = errors.New("please rotate to new segment")

type TailSegment struct {
	SegmentHeader
	Path     string
	readBuf  []byte
	file     *os.File
	readMMap mmap.MMap
	tail     uint64 // writer use atomic to notify readers
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
	segment.file = file
	readMMap, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		countlog.Error("event!segment.failed to mmap as RDONLY", "err", err, "path", path)
		return nil, err
	}
	segment.readMMap = readMMap
	iter := gocodec.NewIterator(readMMap)
	segmentHeader, _ := iter.Copy((*SegmentHeader)(nil)).(*SegmentHeader)
	if iter.Error != nil {
		return nil, iter.Error
	}
	segment.SegmentHeader = *segmentHeader
	segment.readBuf = iter.Buffer()
	segment.Path = path
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
	if segment.readMMap != nil {
		err := segment.readMMap.Unmap()
		if err != nil {
			countlog.Error("event!segment_tail.failed to unmap read", "err", err)
			failed = true
		} else {
			segment.readMMap = nil
		}
	}
	if segment.file != nil {
		err := segment.file.Close()
		if err != nil {
			countlog.Error("event!segment_tail.failed to close file", "err", err)
			failed = true
		} else {
			segment.file = nil
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
	startOffset := reader.tailOffset
	if startOffset == segment.getTail() {
		// tail not moved
		return nil
	}
	fmt.Println(startOffset-segment.StartOffset, segment.StartOffset, startOffset)
	buf := segment.readBuf[startOffset-segment.StartOffset:]
	bufSize := Offset(len(buf))
	iter.Reset(buf)
	newRowsCount := 0
	for {
		currentOffset := startOffset + (bufSize - Offset(len(iter.Buffer())))
		entry, _ := iter.Copy((*Entry)(nil)).(*Entry)
		if iter.Error == io.EOF {
			reader.tailOffset = currentOffset
			reader.tailBlock = &rowBasedBlock{rows: reader.tailRows}
			countlog.Trace("event!segment_tail.read", "newRowsCount", newRowsCount)
			return nil
		}
		if iter.Error != nil {
			return iter.Error
		}
		reader.tailRows = append(reader.tailRows, Row{Entry: entry, Offset: currentOffset})
		newRowsCount++
	}
}
