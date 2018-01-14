package lstore

type Offset uint64
type Blob string

type Entry struct {
	IntValues  []int64
	BlobValues []Blob
}

type Row struct {
	*Entry
	Offset Offset
}