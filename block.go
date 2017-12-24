package lstore

type Row struct {
	*Entry
	Offset Offset
}

type Block interface {
	Search(reader *Reader, startOffset Offset, filters []Filter, collector []Row) ([]Row, error)
}