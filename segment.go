package lstore

import (
	"os"
	"github.com/edsrzf/mmap-go"
	"github.com/v2pro/plz/countlog"
	"github.com/esdb/gocodec"
	"errors"
)

type Blob []byte
type EntryType uint8
type SegmentType uint8
type Offset uint64

type Entry struct {
	Reserved   uint8
	EntryType  EntryType
	IntValues  []int64
	BlobValues []Blob
}

type Segment struct {
	SegmentHeader
	writeMMap mmap.MMap
	writeBuf []byte
	readBuf  []byte
	resources []func() error
	Tail      Offset
}

type SegmentHeader struct {
	SegmentType SegmentType
	StartOffset Offset
}

const SegmentTypeRowBased SegmentType = 1
const SegmentTypeColumnBased SegmentType = 2

const EntryTypeData EntryType = 7
const EntryTypeJunk EntryType = 6
const EntryTypeConfigurationChange = 5

func openSegment(filename string, maxSize int64, startOffset Offset) (*Segment, error) {
	file, err := os.OpenFile(filename, os.O_RDWR, 0666)
	if err != nil {
		file, err = createSegment(filename, maxSize, startOffset)
		if err != nil {
			return nil, err
		}
	}
	segment := &Segment{}
	segment.resources = append(segment.resources, func() error {
		return file.Close()
	})
	writeMMap, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		countlog.Error("event!segment.failed to mmap as RDWR", "err", err, "filename", filename)
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
		countlog.Error("event!segment.failed to mmap as COPY", "err", err, "filename", filename)
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
	return segment, nil
}

func createSegment(filename string, maxSize int64, startOffset Offset) (*os.File, error) {
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

func (segment *Segment) Close() error {
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

func (segment *Segment) ReadBuffer() []byte {
	return segment.readBuf
}

func (segment *Segment) WriteBuffer() []byte {
	return segment.writeBuf
}