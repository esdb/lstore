package lstore

import (
	"github.com/esdb/biter"
	"github.com/esdb/pbloom"
	"unsafe"
)

type Filter interface {
	matches(entry *Entry) bool
	searchIndex(idx slotIndex) biter.Bits
	searchBlock(blk *block, mask []bool)
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

func (filter *IntRangeFilter) searchBlock(blk *block, mask []bool) {
	column := blk.intColumns[filter.Index]
	for i, elem := range column {
		if elem > filter.Max || elem < filter.Min {
			mask[i] = false
		}
	}
}

func (filter *IntRangeFilter) searchIndex(idx slotIndex) biter.Slot {
	return 0
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

func (filter *IntValueFilter) searchBlock(blk *block, mask []bool) {
	column := blk.intColumns[filter.Index]
	for i, elem := range column {
		if elem != filter.Value {
			mask[i] = false
		}
	}
}

func (filter *IntValueFilter) searchIndex(idx slotIndex) biter.Slot {
	return 0
}

type blobValueFilter struct {
	sourceColumn  int
	indexedColumn int
	value         Blob
	valueHash     uint32
	hashed        pbloom.HashedElement
	bloom         pbloom.BloomElement
}

func (store *Store) NewBlobValueFilter(
	column int, value Blob) Filter {
	indexingStrategy := store.blockManager.indexingStrategy
	hashingStrategy := store.hashingStrategy
	indexedColumn := indexingStrategy.lookupBlobColumn(column)
	hashed := hashingStrategy.HashStage1(*(*[]byte)((unsafe.Pointer)(&value)))
	bloom := hashingStrategy.HashStage2(hashed)
	return &blobValueFilter{
		sourceColumn:  column,
		indexedColumn: indexedColumn,
		value:         value,
		valueHash:     hashed.DownCastToUint32(),
		hashed:        hashed,
		bloom:         bloom,
	}
}

func (filter *blobValueFilter) matches(entry *Entry) bool {
	return entry.BlobValues[filter.sourceColumn] == filter.value
}

func (filter *blobValueFilter) searchBlock(blk *block, mask []bool) {
	column := blk.blobColumns[filter.sourceColumn]
	hashColumn := blk.blobHashColumns[filter.indexedColumn]
	for i, elem := range hashColumn {
		if elem != filter.valueHash {
			mask[i] = false
			continue
		}
		if column[i] != filter.value {
			mask[i] = false
		}
	}
}

func (filter *blobValueFilter) searchIndex(idx slotIndex) biter.Bits {
	pbf := idx.pbfs[filter.indexedColumn]
	return pbf.Find(filter.bloom)
}
