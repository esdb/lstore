package lstore

import (
	"github.com/esdb/biter"
	"github.com/esdb/pbloom"
	"unsafe"
	"github.com/v2pro/plz/countlog"
)

type rawChunkRoot struct {
	pbfs     []pbloom.ParallelBloomFilter // 64 slots
	headSlot biter.Slot                   // some slots will be deleted, after entries moved into index segment
	tailSlot biter.Slot
}

type rawChunkChild struct {
	pbfs     []pbloom.ParallelBloomFilter // 64 slots
	children []*Entry
	tailSlot biter.Slot
}

// rawChunk only resides in memory
type rawChunk struct {
	rawChunkRoot
	headOffset Offset
	tailOffset Offset
	children   []*rawChunkChild
	strategy   *IndexingStrategy
}

func newRawChunks(strategy *IndexingStrategy, headOffset Offset) []*rawChunk {
	return []*rawChunk{newRawChunk(strategy, headOffset)}
}

func newRawChunk(strategy *IndexingStrategy, headOffset Offset) *rawChunk {
	children := make([]*rawChunkChild, 64)
	for i := 0; i < len(children); i++ {
		child := &rawChunkChild{nil, make([]*Entry, 64), 0}
		children[i] = child
	}
	hashingStrategy := strategy.tinyHashingStrategy
	pbfs := make([]pbloom.ParallelBloomFilter, strategy.bloomFilterIndexedColumnsCount())
	for i := 0; i < len(pbfs); i++ {
		pbfs[i] = hashingStrategy.New()
	}
	return &rawChunk{
		strategy:     strategy,
		headOffset:   headOffset,
		tailOffset:   headOffset,
		children:     children,
		rawChunkRoot: rawChunkRoot{pbfs, 0, 0},
	}
}

func (chunk *rawChunk) add(entry *Entry) bool {
	child := chunk.children[chunk.tailSlot]
	strategy := chunk.strategy
	if child.tailSlot == 63 {
		chunk.tailSlot += 1
		if countlog.ShouldLog(countlog.LevelTrace) {
			countlog.Trace("event!rawChunk.rotated child",
				"chunkHeadOffset", chunk.headOffset,
				"chunkTailOffset", chunk.tailOffset,
				"chunkTailSlot", chunk.tailSlot)
		}
		child = chunk.children[chunk.tailSlot]
	}
	if child.pbfs == nil {
		hashingStrategy := strategy.tinyHashingStrategy
		pbfs := make([]pbloom.ParallelBloomFilter, strategy.bloomFilterIndexedColumnsCount())
		for i := 0; i < len(pbfs); i++ {
			pbfs[i] = hashingStrategy.New()
		}
		child.pbfs = pbfs
	} else {
		child.tailSlot++
	}
	for _, bfIndexedColumn := range strategy.bloomFilterIndexedBlobColumns {
		indexedColumn := bfIndexedColumn.IndexedColumn()
		sourceColumn := bfIndexedColumn.SourceColumn()
		sourceValue := entry.BlobValues[sourceColumn]
		asSlice := *(*[]byte)(unsafe.Pointer(&sourceValue))
		bloom := strategy.tinyHashingStrategy.Hash(asSlice)
		chunk.pbfs[indexedColumn].Put(biter.SetBits[chunk.tailSlot], bloom)
		child.pbfs[indexedColumn].Put(biter.SetBits[child.tailSlot], bloom)
	}
	child.children[child.tailSlot] = entry
	chunk.tailOffset += 1
	countlog.Trace("event!rawChunk.add",
		"chunkHeadOffset", chunk.headOffset,
		"chunkTailOffset", chunk.tailOffset,
		"chunkTailSlot", chunk.tailSlot,
		"childTailSlot", child.tailSlot)
	if chunk.tailSlot == 63 && child.tailSlot == 63 {
		return true
	}
	return false
}

func (chunk *rawChunk) searchForward(ctx countlog.Context, startOffset Offset,
	filter Filter, cb SearchCallback) error {
	rootResult := filter.searchTinyIndex(chunk.pbfs)
	rootResult &= biter.SetBitsForwardUntil[chunk.tailSlot]
	delta := startOffset - chunk.headOffset
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
		childResult := filter.searchTinyIndex(child.pbfs)
		childResult &= biter.SetBitsForwardUntil[child.tailSlot]
		baseOffset := chunk.headOffset + (Offset(rootSlot) << 6) // * 64
		delta := startOffset - baseOffset
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
			if filter.matchesEntry(entry) {
				if err := cb.HandleRow(baseOffset+Offset(childSlot), entry); err != nil {
					return err
				}
			}
		}
	}
}
