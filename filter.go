package lstore

import "bytes"

type Filter interface {
	matches(entry *Entry) bool
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

// IntValueFilter == Value
type IntValueFilter struct {
	Index int
	Value int64
}

// FloatRangeFilter [Min, Max]
type FloatRangeFilter struct {
	Index int
	Min   float64
	Max   float64
}

// FloatValueFilter == Value
type FloatValueFilter struct {
	Index int
	Value float64
}

// BlobValueFilter == Value
type BlobValueFilter struct {
	Index int
	Value Blob
}

func (filter *BlobValueFilter) matches(entry *Entry) bool {
	return bytes.Equal(entry.BlobValues[filter.Index], filter.Value)
}

type AndFilter struct {
	Filters []Filter
}

func (filter *AndFilter) matches(entry *Entry) bool {
	for _, filter := range filter.Filters {
		if !filter.matches(entry) {
			return false
		}
	}
	return true
}