package lstore

import (
	"github.com/esdb/pbloom"
	"github.com/esdb/biter"
)

type slotIndex struct {
	pbfs         []pbloom.ParallelBloomFilter // 64 slots
}

func newSlotIndex(indexingStrategy *indexingStrategy,
	hashingStrategy *pbloom.HashingStrategy) slotIndex {
	pbfs := make([]pbloom.ParallelBloomFilter, indexingStrategy.bloomFilterIndexedColumnsCount())
	for i := 0; i < len(pbfs); i++ {
		pbfs[i] = hashingStrategy.New()
	}
	return slotIndex{pbfs}
}

func (idx slotIndex) copy() slotIndex {
	newVersion := slotIndex{}
	newVersion.pbfs = make([]pbloom.ParallelBloomFilter, len(idx.pbfs))
	for i := 0; i < len(newVersion.pbfs); i++ {
		newVersion.pbfs[i] = append(pbloom.ParallelBloomFilter(nil), idx.pbfs[i]...)
	}
	return newVersion
}

func (idx slotIndex) search(filters []Filter) biter.Bits {
	result := biter.SetAllBits
	for _, filter := range filters {
		result &= filter.searchIndex(idx)
	}
	return result
}