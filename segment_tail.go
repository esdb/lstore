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
)

var SegmentOverflowError = errors.New("please rotate to new chunk")

type TailSegment struct {
	SegmentHeader
	*ref.ReferenceCounted
	path     string
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
	resources := []io.Closer{file}
	segment := &TailSegment{}
	segment.file = file
	readMMap, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		file.Close()
		countlog.Error("event!chunk.failed to mmap as RDONLY", "err", err, "path", path)
		return nil, err
	}
	resources = append(resources, ref.NewResource("tail chunk readMMap", func() error {
		return readMMap.Unmap()
	}))
	iter := gocodec.NewIterator(readMMap)
	segmentHeader, _ := iter.Copy((*SegmentHeader)(nil)).(*SegmentHeader)
	if iter.Error != nil {
		readMMap.Unmap()
		file.Close()
		return nil, iter.Error
	}
	segment.SegmentHeader = *segmentHeader
	segment.readBuf = iter.Buffer()
	segment.path = path
	segment.ReferenceCounted = ref.NewReferenceCounted("tail chunk", resources...)
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
		reader.tailRows = append(reader.tailRows, Row{Entry: entry, Seq: currentSeq})
		newRowsCount++
	}
}