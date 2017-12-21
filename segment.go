package lstore

import (
	"github.com/esdb/gocodec"
	"os"
	"github.com/edsrzf/mmap-go"
	"github.com/v2pro/plz/countlog"
	"errors"
	"bytes"
)

type Segment struct {
	mapped   mmap.MMap
	file     *os.File
	filename string
	size     int64
	tail     Offset
	stream   *gocodec.Stream
	iter     *gocodec.Iterator
}

func NewSegment(filename string) *Segment {
	return &Segment{
		filename: filename,
		size: 256 * 1024 * 1024,
		stream:   gocodec.NewStream(nil),
		iter:     gocodec.NewIterator(nil),
	}
}

type Blob []byte
type RowType uint8

const RowTypeData RowType = 7
const RowTypeJunk RowType = 6
const RowTypeConfigurationChange = 5

var WriteOnceError = errors.New("every offset can only be written once")
var SegmentOverflowError = errors.New("please rotate to new segment")

type Row struct {
	Reserved    uint8
	RowType     RowType
	IntValues   []int64
	FloatValues []float64
	BlobValues  []Blob
}

type Offset uint64

func (segment *Segment) init() error {
	if segment.file != nil {
		return nil
	}
	file, err := os.OpenFile(segment.filename, os.O_RDWR, 0666)
	if err != nil {
		file, err = os.OpenFile(segment.filename, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return err
		}
		err = file.Truncate(segment.size)
		if err != nil {
			return err
		}
	}
	segment.file = file
	mapped, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		countlog.Error("event!segment.failed to mmap", "err", err, "filename", segment.filename)
		return err
	}
	segment.mapped = mapped
	return nil
}

func (segment *Segment) close() error {
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

func (segment *Segment) Append(row Row) (Offset, error) {
	return segment.Write(segment.tail, row)
}

func (segment *Segment) Write(offset Offset, row Row) (Offset, error) {
	err := segment.init()
	if err != nil {
		return 0, err
	}
	if offset != segment.tail {
		return 0, WriteOnceError
	}
	gocStream := segment.stream
	gocStream.Reset(segment.mapped[offset:offset])
	size := gocStream.Marshal(row)
	if gocStream.Error != nil {
		return 0, gocStream.Error
	}
	segment.tail = offset + Offset(size)
	if segment.tail >= Offset(len(segment.mapped)) {
		for i := offset; i < Offset(len(segment.mapped)); i++ {
			segment.mapped[i] = 0
		}
		return segment.tail, SegmentOverflowError
	}
	return segment.tail, nil
}

func (segment *Segment) Read(offset Offset) (*Row, error) {
	err := segment.init()
	if err != nil {
		return nil, err
	}
	iter := segment.iter
	iter.Reset(segment.mapped[offset:])
	row := iter.Unmarshal((*Row)(nil))
	if iter.Error != nil {
		return nil, err
	}
	return row.(*Row), nil
}

func (segment *Segment) ReadMultiple(offset Offset, maxRead int) ([]*Row, error) {
	err := segment.init()
	if err != nil {
		return nil, err
	}
	iter := segment.iter
	iter.Reset(segment.mapped[offset:segment.tail])
	var rows []*Row
	for i := 0; i < maxRead; i++ {
		if len(iter.Buffer()) == 0 {
			return rows, nil
		}
		// reset iter.baseOffset
		iter.Reset(iter.Buffer())
		row := iter.Unmarshal((*Row)(nil))
		if iter.Error != nil {
			return nil, err
		}
		rows = append(rows, row.(*Row))
	}
	return rows, nil
}

type Filter interface {
	matches(row *Row) bool
}

// IntRangeFilter [Min, Max]
type IntRangeFilter struct {
	Index int
	Min   int64
	Max   int64
}

func (filter *IntRangeFilter) matches(row *Row) bool {
	value := row.IntValues[filter.Index]
	return value >= filter.Min && value <= filter.Max
}

// IntValueFilter == Value
type IntValueFilter struct {
	Index int
	Value int64
}

// FloatRangeFilter [Min, Max]
type FloatRangeFilter struct {
	Index int
	Min   float64
	Max   float64
}

// FloatValueFilter == Value
type FloatValueFilter struct {
	Index int
	Value float64
}

// BlobValueFilter == Value
type BlobValueFilter struct {
	Index int
	Value Blob
}

func (filter *BlobValueFilter) matches(row *Row) bool {
	return bytes.Equal(row.BlobValues[filter.Index], filter.Value)
}

type AndFilter struct {
	Filters []Filter
}

func (filter *AndFilter) matches(row *Row) bool {
	for _, filter := range filter.Filters {
		if !filter.matches(row) {
			return false
		}
	}
	return true
}

func (segment *Segment) Scan(startOffset Offset, batchSize int, filter Filter) (func() ([]*Row, error)) {
	err := segment.init()
	iter := segment.iter
	iter.Reset(segment.mapped[startOffset:segment.tail])
	return func() ([]*Row, error) {
		if err != nil {
			return nil, err
		}
		var rows []*Row
		for i := 0; i < batchSize; i++ {
			if len(iter.Buffer()) == 0 {
				return rows, nil
			}
			iter.Reset(iter.Buffer())
			row := iter.Unmarshal((*Row)(nil)).(*Row)
			if iter.Error != nil {
				return nil, iter.Error
			}
			if filter.matches(row) {
				rows = append(rows, row)
			}
		}
		return rows, nil
	}
}
