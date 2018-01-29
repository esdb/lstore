package lstore

import (
	"github.com/esdb/biter"
	"github.com/esdb/pbloom"
	"github.com/v2pro/plz/countlog"
	"unsafe"
)

type chunkRoot struct {
	pbfs     []pbloom.ParallelBloomFilter // 64 slots
	tailSlot biter.Slot
}

type chunkChild struct {
	pbfs     []pbloom.ParallelBloomFilter // 64 slots
	children []*Entry
	tailSlot biter.Slot
}

// chunk only resides in memory
type chunk struct {
	chunkRoot
	headOffset Offset
	tailOffset Offset
	children   []*chunkChild
	strategy   *indexingStrategy
}

func newChunks(strategy *indexingStrategy, headOffset Offset) []*chunk {
	return []*chunk{newChunk(strategy, headOffset)}
}

func newChunk(strategy *indexingStrategy, headOffset Offset) *chunk {
	children := make([]*chunkChild, 64)
	for i := 0; i < len(children); i++ {
		child := &chunkChild{nil, make([]*Entry, 64), 0}
		hashingStrategy := strategy.tinyHashingStrategy
		pbfs := make([]pbloom.ParallelBloomFilter, strategy.bloomFilterIndexedColumnsCount())
		for i := 0; i < len(pbfs); i++ {
			pbfs[i] = hashingStrategy.New()
		}
		child.tailSlot = 0
		child.pbfs = pbfs
		children[i] = child
	}
	hashingStrategy := strategy.tinyHashingStrategy
	pbfs := make([]pbloom.ParallelBloomFilter, strategy.bloomFilterIndexedColumnsCount())
	for i := 0; i < len(pbfs); i++ {
		pbfs[i] = hashingStrategy.New()
	}
	return &chunk{
		strategy:   strategy,
		headOffset: headOffset,
		tailOffset: headOffset,
		children:   children,
		chunkRoot:  chunkRoot{pbfs, 1},
	}
}

func (chunk *chunk) add(entry *Entry) bool {
	chunkCurrentSlot := chunk.tailSlot - 1
	child := chunk.children[chunkCurrentSlot]
	countlog.Trace("event!chunk.add",
		"chunkHeadOffset", chunk.headOffset,
		"chunkTailOffset", chunk.tailOffset,
		"chunkTailSlot", chunk.tailSlot,
		"childTailSlot", child.tailSlot)
	strategy := chunk.strategy
	child.children[child.tailSlot] = entry
	for _, bfIndexedColumn := range strategy.bloomFilterIndexedBlobColumns {
		indexedColumn := bfIndexedColumn.IndexedColumn()
		sourceColumn := bfIndexedColumn.SourceColumn()
		sourceValue := entry.BlobValues[sourceColumn]
		asSlice := *(*[]byte)(unsafe.Pointer(&sourceValue))
		bloom := strategy.tinyHashingStrategy.Hash(asSlice)
		chunk.pbfs[indexedColumn].Put(biter.SetBits[chunkCurrentSlot], bloom)
		child.pbfs[indexedColumn].Put(biter.SetBits[child.tailSlot], bloom)
	}
	chunk.tailOffset += 1
	child.tailSlot += 1
	if child.tailSlot == 64 {
		chunk.tailSlot += 1
		if chunk.tailSlot == 65 {
			chunk.tailSlot = 64
			return true
		}
	}
	return false
}

func (chunk *chunk) searchForward(ctx *countlog.Context, req *SearchRequest) error {
	rootResult := req.Filter.searchTinyIndex(chunk.pbfs)
	rootResult &= biter.SetBitsForwardUntil[chunk.tailSlot]
	delta := req.StartOffset - chunk.headOffset
	if delta < 4096 {
		rootResult &= biter.SetBitsForwardFrom[delta>>6]
	}
	rootIter := rootResult.ScanForward()
	for {
		rootSlot := rootIter()
		if rootSlot == biter.NotFound {
			return nil
		}
		child := chunk.children[rootSlot]
		childResult := req.Filter.searchTinyIndex(child.pbfs)
		childResult &= biter.SetBitsForwardUntil[child.tailSlot]
		baseOffset := chunk.headOffset + (Offset(rootSlot) << 6) // * 64
		delta := req.StartOffset - baseOffset
		if delta < 64 {
			childResult &= biter.SetBitsForwardFrom[delta]
		}
		childIter := childResult.ScanForward()
		for {
			childSlot := childIter()
			if childSlot == biter.NotFound {
				break
			}
			entry := child.children[childSlot]
			if req.Filter.matchesEntry(entry) {
				if err := req.Callback.HandleRow(baseOffset+Offset(childSlot), entry); err != nil {
					return err
				}
			}
		}
	}
}
