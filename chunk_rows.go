package lstore

type rowsChunk []Row

func (chunk rowsChunk) search(reader *Reader, startSeq RowSeq, filters []Filter, collector []Row) ([]Row, error) {
	for _, row := range chunk  {
		if row.Seq < startSeq {
			continue
		}
		if rowMatches(row.Entry, filters) {
			collector = append(collector, row)
		}
	}
	return collector, nil
}

func rowMatches(entry *Entry, filters []Filter) bool {
	for _, filter := range filters {
		if !filter.matches(entry) {
			return false
		}
	}
	return true
}