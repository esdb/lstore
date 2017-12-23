package search

import (
	"bytes"
	"github.com/esdb/lstore"
)

type Filter interface {
	matches(entry *lstore.Entry) bool
}

// IntRangeFilter [Min, Max]
type IntRangeFilter struct {
	Index int
	Min   int64
	Max   int64
}

func (filter *IntRangeFilter) matches(entry *lstore.Entry) bool {
	value := entry.IntValues[filter.Index]
	return value >= filter.Min && value <= filter.Max
}

// IntValueFilter == Value
type IntValueFilter struct {
	Index int
	Value int64
}

// BlobValueFilter == Value
type BlobValueFilter struct {
	Index int
	Value lstore.Blob
}

func (filter *BlobValueFilter) matches(entry *lstore.Entry) bool {
	return bytes.Equal(entry.BlobValues[filter.Index], filter.Value)
}