package lstore

import (
	"github.com/esdb/biter"
	"github.com/esdb/pbloom"
	"github.com/esdb/plinear"
	"github.com/v2pro/plz/countlog"
	"math"
	"unsafe"
)

type Filter interface {
	searchLargeIndex(idx *slotIndex) biter.Bits
	searchMediumIndex(idx *slotIndex) biter.Bits
	searchSmallIndex(idx *slotIndex) biter.Bits
	searchTinyIndex(pbfs []pbloom.ParallelBloomFilter) biter.Bits
	searchBlock(blk *block, beginSlot biter.Slot) biter.Bits
	matchesBlockSlot(blk *block, slot biter.Slot) bool
	matchesEntry(entry *Entry) bool
}

type blobValueFilter struct {
	sourceColumn  int
	indexedColumn int
	value         Blob
	valueHash     uint32
	hashed        pbloom.HashedElement
	tinyBloom     pbloom.BloomElement
	smallBloom    pbloom.BloomElement
	mediumBloom   pbloom.BloomElement
	largeBloom    pbloom.BloomElement
}

func (strategy *indexingStrategy) NewBlobValueFilter(
	column int, value Blob) Filter {
	indexedColumn := strategy.lookupBlobColumn(column)
	hashed := strategy.tinyHashingStrategy.HashStage1(*(*[]byte)((unsafe.Pointer)(&value)))
	return &blobValueFilter{
		sourceColumn:  column,
		indexedColumn: indexedColumn,
		value:         value,
		valueHash:     hashed[0],
		hashed:        hashed,
		tinyBloom:     strategy.tinyHashingStrategy.HashStage2(hashed),
		smallBloom:    strategy.smallHashingStrategy.HashStage2(hashed),
		mediumBloom:   strategy.mediumHashingStrategy.HashStage2(hashed),
		largeBloom:    strategy.largeHashingStrategy.HashStage2(hashed),
	}
}

func (filter *blobValueFilter) matchesEntry(entry *Entry) bool {
	return entry.BlobValues[filter.sourceColumn] == filter.value
}

func (filter *blobValueFilter) matchesBlockSlot(blk *block, slot biter.Slot) bool {
	matches := blk.blobColumns[filter.sourceColumn][slot] == filter.value
	if !matches {
		countlog.Trace("event!slot filtered by matchesBlockSlot",
			"block.StartOff", blk.startOffset,
			"slot", slot,
			"actual", blk.blobColumns[filter.sourceColumn][slot],
			"expected", filter.value)
	}
	return matches
}

func (filter *blobValueFilter) searchBlock(blk *block, begin biter.Slot) biter.Bits {
	hashColumn := blk.blobHashColumns[filter.indexedColumn]
	fragment := (*[64]uint32)(unsafe.Pointer(&hashColumn[begin]))
	return biter.Bits(plinear.CompareEqualByAvx(filter.valueHash, fragment))
}

func (filter *blobValueFilter) searchLargeIndex(idx *slotIndex) biter.Bits {
	pbf := idx.pbfs[filter.indexedColumn]
	return pbf.Find(filter.largeBloom)
}

func (filter *blobValueFilter) searchMediumIndex(idx *slotIndex) biter.Bits {
	pbf := idx.pbfs[filter.indexedColumn]
	return pbf.Find(filter.mediumBloom)
}

func (filter *blobValueFilter) searchSmallIndex(idx *slotIndex) biter.Bits {
	pbf := idx.pbfs[filter.indexedColumn]
	return pbf.Find(filter.smallBloom)
}

func (filter *blobValueFilter) searchTinyIndex(pbfs []pbloom.ParallelBloomFilter) biter.Bits {
	pbf := pbfs[filter.indexedColumn]
	return pbf.Find(filter.tinyBloom)
}

type dummyFilter struct {
}

var dummyFilterInstance = &dummyFilter{}

func (filter *dummyFilter) matchesEntry(entry *Entry) bool {
	return true
}

func (filter *dummyFilter) matchesBlockSlot(blk *block, slot biter.Slot) bool {
	return true
}

func (filter *dummyFilter) searchBlock(blk *block, begin biter.Slot) biter.Bits {
	return math.MaxUint64
}

func (filter *dummyFilter) searchLargeIndex(idx *slotIndex) biter.Bits {
	return math.MaxUint64
}

func (filter *dummyFilter) searchMediumIndex(idx *slotIndex) biter.Bits {
	return math.MaxUint64
}

func (filter *dummyFilter) searchSmallIndex(idx *slotIndex) biter.Bits {
	return math.MaxUint64
}

func (filter *dummyFilter) searchTinyIndex(pbfs []pbloom.ParallelBloomFilter) biter.Bits {
	return math.MaxUint64
}
