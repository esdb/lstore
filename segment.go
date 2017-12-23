package lstore

import (
	"os"
	"github.com/edsrzf/mmap-go"
	"github.com/v2pro/plz/countlog"
)

type Blob []byte
type EntryType uint8
type Offset uint64

type Entry struct {
	Reserved    uint8
	EntryType   EntryType
	IntValues   []int64
	BlobValues  []Blob
}

type Segment struct {
	writeMMap mmap.MMap
	readMMap mmap.MMap
	file   *os.File
	Tail   Offset
}

const EntryTypeData EntryType = 7
const EntryTypeJunk EntryType = 6
const EntryTypeConfigurationChange = 5

func openSegment(filename string, maxSize int64) (*Segment, error) {
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
	segment := &Segment{}
	segment.file = file
	mapped, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		countlog.Error("event!segment.failed to mmap as RDWR", "err", err, "filename", filename)
		return nil, err
	}
	segment.writeMMap = mapped
	mapped, err = mmap.Map(file, mmap.COPY, 0)
	if err != nil {
		countlog.Error("event!segment.failed to mmap as COPY", "err", err, "filename", filename)
		return nil, err
	}
	segment.readMMap = mapped
	return segment, nil
}

func (segment *Segment) Close() error {
	if segment.writeMMap != nil {
		err := segment.writeMMap.Unmap()
		if err != nil {
			countlog.Error("event!segment.failed to unmap write", "err", err)
			return err
		}
	}
	if segment.readMMap != nil {
		err := segment.readMMap.Unmap()
		if err != nil {
			countlog.Error("event!segment.failed to unmap read", "err", err)
			return err
		}
	}
	if segment.file != nil {
		err := segment.file.Close()
		if err != nil {
			countlog.Error("event!segment.failed to close file", "err", err)
			return err
		}
	}
	return nil
}

func (segment *Segment) ReadMMap() []byte {
	return []byte(segment.readMMap)
}

func (segment *Segment) WriteMMap() []byte {
	return []byte(segment.writeMMap)
}