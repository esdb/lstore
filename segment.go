package lstore

type Blob []byte
type EntryType uint8
type SegmentType uint8
type RowSeq uint64

type Entry struct {
	Reserved   uint8
	EntryType  EntryType
	IntValues  []int64
	BlobValues []Blob
}
type SegmentHeader struct {
	SegmentType SegmentType
	StartSeq RowSeq
}

const SegmentTypeRowBased SegmentType = 1
const SegmentTypeColumnBased SegmentType = 2

const EntryTypeData EntryType = 7
const EntryTypeJunk EntryType = 6
const EntryTypeConfigurationChange = 5

type Row struct {
	*Entry
	Seq RowSeq
}

type segment interface {
	search(reader *Reader, startSeq RowSeq, filters []Filter, collector []Row) ([]Row, error)
}