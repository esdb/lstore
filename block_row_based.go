package lstore

type rowBasedBlock struct {
	rows []Row
}

func (blk *rowBasedBlock) search(reader *Reader, startOffset Offset, filters []Filter, collector []Row) ([]Row, error) {
	for _, row := range blk.rows  {
		if row.Offset < startOffset {
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