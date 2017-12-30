package lstore

type Offset uint64
type Blob string
type EntryType uint8

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
	Offset Offset
}