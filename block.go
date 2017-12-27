package lstore

type Row struct {
	*Entry
	Offset Offset
}

type block interface {
	search(reader *Reader, startOffset Offset, filters []Filter, collector []Row) ([]Row, error)
}