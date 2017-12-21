package lstore

import (
	"github.com/esdb/gocodec"
	"os"
	"github.com/edsrzf/mmap-go"
	"github.com/v2pro/plz/countlog"
	"errors"
)

type Blob []byte
type EntryType uint8
type Offset uint64

type Entry struct {
	Reserved    uint8
	EntryType   EntryType
	IntValues   []int64
	FloatValues []float64
	BlobValues  []Blob
}

type Row struct {
	*Entry
	Offset Offset
}

type Segment struct {
	mapped mmap.MMap
	rows   []Row
	file   *os.File
	tail   Offset
	stream *gocodec.Stream
	iter   *gocodec.Iterator
}

const EntryTypeData EntryType = 7
const EntryTypeJunk EntryType = 6
const EntryTypeConfigurationChange = 5

var WriteOnceError = errors.New("every offset can only be written once")
var SegmentOverflowError = errors.New("please rotate to new segment")

func OpenSegment(filename string, maxSize int64) (*Segment, error) {
	file, err := os.OpenFile(filename, os.O_RDWR, 0666)
	if err != nil {
		file, err = os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return nil, err
		}
		err = file.Truncate(maxSize)
		if err != nil {
			return nil, err
		}
	}
	segment := &Segment{
		stream: gocodec.NewStream(nil),
		iter:   gocodec.NewIterator(nil),
	}
	segment.file = file
	mapped, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		countlog.Error("event!segment.failed to mmap", "err", err, "filename", filename)
		return nil, err
	}
	segment.mapped = mapped
	return segment, nil
}

func (segment *Segment) Close() error {
	if segment.mapped != nil {
		err := segment.mapped.Unmap()
		if err != nil {
			return err
		}
	}
	if segment.file != nil {
		err := segment.file.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (segment *Segment) Append(entry *Entry) (Offset, error) {
	return segment.Write(segment.tail, entry)
}

func (segment *Segment) Write(offset Offset, entry *Entry) (Offset, error) {
	if segment.tail >= Offset(len(segment.mapped)) {
		return 0, SegmentOverflowError
	}
	if offset != segment.tail {
		return 0, WriteOnceError
	}
	gocStream := segment.stream
	gocStream.Reset(segment.mapped[offset:offset])
	size := gocStream.Marshal(*entry)
	if gocStream.Error != nil {
		return 0, gocStream.Error
	}
	segment.tail = offset + Offset(size)
	if segment.tail >= Offset(len(segment.mapped)) {
		return 0, SegmentOverflowError
	}
	segment.rows = append(segment.rows, Row{Offset: offset, Entry: entry})
	return segment.tail, nil
}

func (segment *Segment) search(offset Offset) (int, error) {
	rows := segment.rows
	start := 0
	end := len(segment.rows) - 1
	for start <= end {
		median := (start + end) / 2
		if rows[median].Offset < offset {
			start = median + 1
		} else {
			end = median - 1
		}
	}
	if start == len(rows) || rows[start].Offset != offset {
		return -1, errors.New("not found")
	}
	return start, nil
}

func (segment *Segment) Read(offset Offset) (Row, error) {
	i, err := segment.search(offset)
	if err != nil {
		return Row{}, err
	}
	return segment.rows[i], nil
}

func (segment *Segment) ReadMultiple(offset Offset, maxRead int) ([]Row, error) {
	start, err := segment.search(offset)
	if err != nil {
		return nil, err
	}
	rows := segment.rows
	end := start + maxRead
	if end > len(rows) {
		end = len(rows)
	}
	return segment.rows[start: end], nil
}

func (segment *Segment) Scan(startOffset Offset, batchSize int, filter Filter) (func() ([]Row, error)) {
	start, err := segment.search(startOffset)
	return func() ([]Row, error) {
		if err != nil {
			return nil, err
		}
		current := start
		var batch []Row
		for len(batch) < batchSize {
			if current >= len(segment.rows) {
				break
			}
			row := segment.rows[current]
			if filter.matches(row.Entry) {
				batch = append(batch, row)
			}
			current++
		}
		return batch, nil
	}
}
