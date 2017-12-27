package lstore

type Row struct {
	*Entry
	Seq RowSeq
}

type block interface {
	search(reader *Reader, startSeq RowSeq, filters []Filter, collector []Row) ([]Row, error)
}