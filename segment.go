package lstore

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
type SegmentHeader struct {
	SegmentType SegmentType
	StartOffset Offset
}

const SegmentTypeRowBased SegmentType = 1
const SegmentTypeColumnBased SegmentType = 2

const EntryTypeData EntryType = 7
const EntryTypeJunk EntryType = 6
const EntryTypeConfigurationChange = 5
