package lstore

type rowsSegment struct {
	rows []Row
}

func (segment *rowsSegment) search(reader *Reader, startSeq RowSeq, filters []Filter, collector []Row) ([]Row, error) {
	for _, row := range segment.rows  {
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