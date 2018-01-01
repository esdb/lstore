package lstore

import (
	"github.com/esdb/pbloom"
	"github.com/esdb/biter"
)

type slotIndexSeq uint64

type slotIndex struct {
	pbfs []pbloom.ParallelBloomFilter // 64 slots
	children []uint64 // 64 slots, can be blockSeq or slotIndexSeq
}

func newSlotIndex(indexingStrategy *indexingStrategy,
	hashingStrategy *pbloom.HashingStrategy) *slotIndex {
	pbfs := make([]pbloom.ParallelBloomFilter, indexingStrategy.bloomFilterIndexedColumnsCount())
	for i := 0; i < len(pbfs); i++ {
		pbfs[i] = hashingStrategy.New()
	}
	return &slotIndex{pbfs, nil}
}

func (idx *slotIndex) copy() *slotIndex {
	newVersion := &slotIndex{}
	newVersion.pbfs = make([]pbloom.ParallelBloomFilter, len(idx.pbfs))
	for i := 0; i < len(newVersion.pbfs); i++ {
		newVersion.pbfs[i] = append(pbloom.ParallelBloomFilter(nil), idx.pbfs[i]...)
	}
	newVersion.children = append([]uint64(nil), idx.children...)
	return newVersion
}

func (idx *slotIndex) searchLarge(filters []Filter) biter.Bits {
	result := biter.SetAllBits
	for _, filter := range filters {
		result &= filter.searchLargeIndex(idx)
	}
	return result
}

func (idx *slotIndex) searchMedium(filters []Filter) biter.Bits {
	result := biter.SetAllBits
	for _, filter := range filters {
		result &= filter.searchMediumIndex(idx)
	}
	return result
}

func (idx *slotIndex) searchSmall(filters []Filter) biter.Bits {
	result := biter.SetAllBits
	for _, filter := range filters {
		result &= filter.searchSmallIndex(idx)
	}
	return result
}
