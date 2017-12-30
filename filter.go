package lstore

import (
	"github.com/esdb/biter"
	"github.com/esdb/pbloom"
	"unsafe"
)

type Filter interface {
	searchBigIndex(idx slotIndex) biter.Bits
	searchSmallIndex(idx slotIndex) biter.Bits
	searchBlock(blk *block, mask []bool)
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
	smallBloom    pbloom.BloomElement
	bigBloom      pbloom.BloomElement
}

func (store *Store) NewBlobValueFilter(
	column int, value Blob) Filter {
	strategy := store.blockManager.indexingStrategy
	indexedColumn := strategy.lookupBlobColumn(column)
	hashed := strategy.smallHashingStrategy.HashStage1(*(*[]byte)((unsafe.Pointer)(&value)))
	return &blobValueFilter{
		sourceColumn:  column,
		indexedColumn: indexedColumn,
		value:         value,
		valueHash:     hashed.DownCastToUint32(),
		hashed:        hashed,
		smallBloom:    strategy.smallHashingStrategy.HashStage2(hashed),
		bigBloom:      strategy.bigHashingStrategy.HashStage2(hashed),
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

func (filter *blobValueFilter) searchBigIndex(idx slotIndex) biter.Bits {
	pbf := idx.pbfs[filter.indexedColumn]
	return pbf.Find(filter.smallBloom)
}

func (filter *blobValueFilter) searchSmallIndex(idx slotIndex) biter.Bits {
	pbf := idx.pbfs[filter.indexedColumn]
	return pbf.Find(filter.smallBloom)
}
