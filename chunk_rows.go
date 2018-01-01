package lstore

import (
	"strconv"
	"github.com/v2pro/plz/countlog"
)

type rowsChunk struct {
	startOffset Offset
	rows        []*Entry
}

func newRowsChunk(startOffset Offset) rowsChunk {
	return rowsChunk{
		startOffset: startOffset,
		rows:        make([]*Entry, 0, blockLength),
	}
}

func (chunk rowsChunk) search(ctx countlog.Context, collector []Row, startOffset Offset, filters ...Filter) ([]Row, error) {
	for i, entry := range chunk.rows {
		offset := chunk.startOffset + Offset(i)
		if offset < startOffset {
			continue
		}
		if rowMatches(entry, filters) {
			collector = append(collector, Row{Entry: entry, Offset: offset})
		}
	}
	return collector, nil
}

func (chunk rowsChunk) String() string {
	if len(chunk.rows) == 0 {
		return "rowsChunk{}"
	}
	start := int(chunk.startOffset)
	end := start + len(chunk.rows) - 1
	desc := "rowsChunk{" + strconv.Itoa(start) + "~" + strconv.Itoa(end) + "}"
	return desc
}

func rowMatches(entry *Entry, filters []Filter) bool {
	for _, filter := range filters {
		if !filter.matches(entry) {
			return false
		}
	}
	return true
}
