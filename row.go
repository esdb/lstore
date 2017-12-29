package lstore

// the sequence number for entries
type RowSeq uint64
type Blob string
type EntryType uint8
type SegmentType uint8

type Entry struct {
	Reserved   uint8
	EntryType  EntryType
	IntValues  []int64
	BlobValues []Blob
}

const EntryTypeData EntryType = 7
const EntryTypeJunk EntryType = 6
const EntryTypeConfigurationChange = 5

type Row struct {
	*Entry
	Seq RowSeq
}