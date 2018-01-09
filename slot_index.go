package lstore

import (
	"github.com/esdb/pbloom"
	"github.com/esdb/biter"
)

type slotIndexSeq uint64

type slotIndex struct {
	pbfs     []pbloom.ParallelBloomFilter // 64 slots
	children []uint64                     // 64 slots, can be blockSeq or slotIndexSeq
	tailSlot *biter.Slot
}

func newSlotIndex(indexingStrategy *IndexingStrategy, level level) *slotIndex {
	hashingStrategy := indexingStrategy.hashingStrategy(level)
	pbfs := make([]pbloom.ParallelBloomFilter, indexingStrategy.bloomFilterIndexedColumnsCount())
	for i := 0; i < len(pbfs); i++ {
		pbfs[i] = hashingStrategy.New()
	}
	var tailSlot biter.Slot = 0
	return &slotIndex{pbfs, make([]uint64, 64), &tailSlot}
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

func (idx *slotIndex) setTailSlot(tailSlot biter.Slot) {
	*idx.tailSlot = tailSlot
}

func (idx *slotIndex) updateSlot(slotMask biter.Bits, child *slotIndex) {
	for i, pbf := range child.pbfs {
		parentPbf := idx.pbfs[i]
		for loc, bits := range pbf {
			// any child slot is set, the slot is set
			if bits != 0 {
				parentPbf[loc] = slotMask
			}
		}
	}
}

func (idx *slotIndex) search(level level, filter Filter) biter.Bits {
	switch level {
	case level0:
		return filter.searchSmallIndex(idx)
	case level1:
		return filter.searchMediumIndex(idx)
	default:
		return filter.searchLargeIndex(idx)
	}
}
