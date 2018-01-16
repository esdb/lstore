package lstore

import (
	"unsafe"
	"sync/atomic"
	"github.com/v2pro/plz/countlog"
)

type storeState struct {
	currentVersion   unsafe.Pointer // pointer to storeVersion, appender use atomic to notify readers
	tailOffset       uint64         // offset, appender use atomic to notify readers
	blockManager     blockManager
	slotIndexManager slotIndexManager
}

type storeVersion struct {
	activeReaders    map[interface{}]Offset
	removingSegments []*indexSegment
	indexedSegments  []*indexSegment
	indexingSegment  *indexSegment
	appendedChunks   []*chunk
	appendingChunk   *chunk
}

func (version *storeVersion) HeadOffset() Offset {
	if len(version.indexedSegments) != 0 {
		return version.indexedSegments[0].headOffset
	}
	return version.indexingSegment.headOffset
}

func (store *storeState) getTailOffset() Offset {
	return Offset(atomic.LoadUint64(&store.tailOffset))
}

// latest does not guarantee the indexedSegments will not be removed
func (store *storeState) latest() *storeVersion {
	version := (*storeVersion)(atomic.LoadPointer(&store.currentVersion))
	return version
}

// lockHead will lock the indexedSegments from removing
func (store *storeState) lockHead(reader interface{}) *storeVersion {
	for {
		oldVersion := store.latest()
		newVersion := *oldVersion
		newActiveReaders := make(map[interface{}]Offset, len(newVersion.activeReaders))
		for k, v := range newVersion.activeReaders {
			newActiveReaders[k] = v
		}
		newActiveReaders[reader] = newVersion.HeadOffset()
		newVersion.activeReaders = newActiveReaders
		if atomic.CompareAndSwapPointer(&store.currentVersion, unsafe.Pointer(oldVersion), unsafe.Pointer(&newVersion)) {
			return &newVersion
		}
	}
}

func (store *storeState) unlockHead(reader interface{}) {
	for {
		oldVersion := store.latest()
		newVersion := *oldVersion
		newActiveReaders := make(map[interface{}]Offset, len(newVersion.activeReaders))
		for k, v := range newVersion.activeReaders {
			newActiveReaders[k] = v
		}
		delete(newActiveReaders, reader)
		newVersion.activeReaders = newActiveReaders
		if atomic.CompareAndSwapPointer(&store.currentVersion, unsafe.Pointer(oldVersion), unsafe.Pointer(&newVersion)) {
			return
		}
	}
}

func (store *storeState) removeHead(removingFrom Offset) ([]*indexSegment, Offset) {
	for {
		oldVersion := store.latest()
		newVersion := *oldVersion
		removedFrom := removingFrom
		for _, lockedHead := range newVersion.activeReaders {
			if lockedHead < removedFrom {
				removedFrom = lockedHead
			}
		}
		var removedSegments []*indexSegment
		var removingSegments []*indexSegment
		var remainingSegments []*indexSegment
		for _, segment := range newVersion.removingSegments {
			if segment.headOffset < removedFrom {
				removedSegments = append(removedSegments, segment)
			} else {
				removingSegments = append(removingSegments, segment)
			}
		}
		for _, segment := range newVersion.indexedSegments {
			if segment.headOffset < removedFrom {
				removedSegments = append(removedSegments, segment)
			} else if segment.headOffset < removingFrom {
				removingSegments = append(removingSegments, segment)
			} else {
				remainingSegments = append(remainingSegments, segment)
			}
		}
		newVersion.indexedSegments = remainingSegments
		newVersion.removingSegments = removingSegments
		if atomic.CompareAndSwapPointer(&store.currentVersion, unsafe.Pointer(oldVersion), unsafe.Pointer(&newVersion)) {
			return removedSegments, removedFrom
		}
	}
}

func (store *storeState) movedChunksIntoIndex(indexingSegment *indexSegment, movedChunksCount int) {
	for {
		oldVersion := store.latest()
		newVersion := *oldVersion
		newVersion.indexingSegment = indexingSegment
		newVersion.appendedChunks = oldVersion.appendedChunks[movedChunksCount:]
		if atomic.CompareAndSwapPointer(&store.currentVersion, unsafe.Pointer(oldVersion), unsafe.Pointer(&newVersion)) {
			countlog.Debug("event!state.moved chunks into index",
				"indexingTailOffset", indexingSegment.tailOffset,
				"appendedChunksCount", len(newVersion.appendedChunks),
				"appendingHeadOffset", newVersion.appendingChunk.headOffset)
			return
		}
	}
}

func (store *storeState) rotatedIndex(indexedSegment *indexSegment, indexingSegment *indexSegment) {
	for {
		oldVersion := store.latest()
		newVersion := *oldVersion
		newVersion.indexedSegments = append(oldVersion.indexedSegments, indexedSegment)
		newVersion.indexingSegment = indexingSegment
		if atomic.CompareAndSwapPointer(&store.currentVersion, unsafe.Pointer(oldVersion), unsafe.Pointer(&newVersion)) {
			countlog.Debug("event!state.rotated index",
				"indexedSegmentHeadOffset", indexedSegment.headOffset,
				"indexingSegmentTailOffset", indexedSegment.tailOffset)
			return
		}
	}
}

func (store *storeState) rotatedChunk(chunk *chunk) {
	for {
		oldVersion := store.latest()
		newVersion := *oldVersion
		newVersion.appendedChunks = append(newVersion.appendedChunks, oldVersion.appendingChunk)
		newVersion.appendingChunk = chunk
		if atomic.CompareAndSwapPointer(&store.currentVersion, unsafe.Pointer(oldVersion), unsafe.Pointer(&newVersion)) {
			countlog.Debug("event!state.rotated chunk",
				"appendingHeadOffset", chunk.headOffset)
			return
		}
	}
}

func (store *storeState) loaded(indexedSegments []*indexSegment, indexingSegment *indexSegment, chunks []*chunk) {
	version := &storeVersion{
		indexedSegments: indexedSegments,
		indexingSegment: indexingSegment,
		appendedChunks:  chunks[:len(chunks)-1],
		appendingChunk:  chunks[len(chunks)-1],
	}
	atomic.StorePointer(&store.currentVersion, unsafe.Pointer(version))
	countlog.Debug("event!state.loaded",
		"headOffset", version.HeadOffset(),
		"tailOffset", store.tailOffset,
		"indexingHeadOffset", indexingSegment.headOffset,
		"indexingTailOffset", indexingSegment.tailOffset)
}
