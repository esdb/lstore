package lstore

type chunk interface {
	search(reader *Reader, startSeq RowSeq, filters []Filter, collector []Row) ([]Row, error)
}