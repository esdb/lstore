package lstore

import (
	"bytes"
)

type Filter interface {
	matches(entry *Entry) bool
	updateMask(blk *block, mask []bool)
}

// IntRangeFilter [Min, Max]
type IntRangeFilter struct {
	Index int
	Min   int64
	Max   int64
}

func (filter *IntRangeFilter) matches(entry *Entry) bool {
	value := entry.IntValues[filter.Index]
	return value >= filter.Min && value <= filter.Max
}

func (filter *IntRangeFilter) updateMask(blk *block, mask []bool) {
	column := blk.intColumns[filter.Index]
	for i, elem := range column {
		if elem > filter.Max || elem < filter.Min {
			mask[i] = false
		}
	}
}

// IntValueFilter == Value
type IntValueFilter struct {
	Index int
	Value int64
}

func (filter *IntValueFilter) matches(entry *Entry) bool {
	value := entry.IntValues[filter.Index]
	return value == filter.Value
}

func (filter *IntValueFilter) updateMask(blk *block, mask []bool) {
	column := blk.intColumns[filter.Index]
	for i, elem := range column {
		if elem != filter.Value {
			mask[i] = false
		}
	}
}

// BlobValueFilter == Value
type BlobValueFilter struct {
	Index     int
	ValueHash uint64
	Value     Blob
}

func (filter *BlobValueFilter) matches(entry *Entry) bool {
	return bytes.Equal(entry.BlobValues[filter.Index], filter.Value)
}

func (filter *BlobValueFilter) updateMask(blk *block, mask []bool) {
	column := blk.blobColumns[filter.Index]
	hashColumn := blk.blobHashColumns[filter.Index]
	for i, elem := range hashColumn {
		if elem != filter.ValueHash {
			mask[i] = false
			continue
		}
		if !bytes.Equal(column[i], filter.Value) {
			mask[i] = false
		}
	}
}
