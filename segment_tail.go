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
	Path     string
	readBuf  []byte
	file     *os.File
	readMMap mmap.MMap
	tail     uint64 // writer use atomic to notify readers
}

func openTailSegment(path string, maxSize int64, startSeq RowSeq) (*TailSegment, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0666)
	if os.IsNotExist(err) {
		file, err = createTailSegment(path, maxSize, startSeq)
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	segment := &TailSegment{}
	segment.file = file
	readMMap, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		file.Close()
		countlog.Error("event!segment.failed to mmap as RDONLY", "err", err, "path", path)
		return nil, err
	}
	segment.readMMap = readMMap
	iter := gocodec.NewIterator(readMMap)
	segmentHeader, _ := iter.Copy((*SegmentHeader)(nil)).(*SegmentHeader)
	if iter.Error != nil {
		readMMap.Unmap()
		file.Close()
		return nil, iter.Error
	}
	segment.SegmentHeader = *segmentHeader
	segment.readBuf = iter.Buffer()
	segment.Path = path
	return segment, nil
}

func createTailSegment(filename string, maxSize int64, startSeq RowSeq) (*os.File, error) {
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
	stream.Marshal(SegmentHeader{SegmentType: SegmentTypeRowBased, StartSeq: startSeq})
	if stream.Error != nil {
		return nil, stream.Error
	}
	_, err = file.Write(stream.Buffer())
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

func (segment *TailSegment) updateTail(tail RowSeq) {
	atomic.StoreUint64(&segment.tail, uint64(tail))
}

func (segment *TailSegment) getTail() RowSeq {
	return RowSeq(atomic.LoadUint64(&segment.tail))
}

func (segment *TailSegment) read(reader *Reader) (bool, error) {
	iter := reader.gocIter
	startSeq := reader.tailSeq
	if startSeq == segment.getTail() {
		// tail not moved
		return false, nil
	}
	buf := segment.readBuf[startSeq-segment.StartSeq:]
	bufSize := RowSeq(len(buf))
	iter.Reset(buf)
	newRowsCount := 0
	for {
		currentSeq := startSeq + (bufSize - RowSeq(len(iter.Buffer())))
		entry, _ := iter.Copy((*Entry)(nil)).(*Entry)
		if iter.Error == io.EOF {
			reader.tailSeq = currentSeq
			countlog.Trace("event!segment_tail.read", "newRowsCount", newRowsCount)
			return true, nil
		}
		if iter.Error != nil {
			return false, iter.Error
		}
		reader.tailRows.rows = append(reader.tailRows.rows, Row{Entry: entry, Seq: currentSeq})
		newRowsCount++
	}
}

func (segment *TailSegment) search(reader *Reader, startSeq RowSeq, filters []Filter, collector []Row) ([]Row, error) {
	panic("tail segment is shared and not searchable without reader")
}