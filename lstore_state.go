package lstore

import (
	"unsafe"
	"sync/atomic"
	"github.com/v2pro/plz/countlog"
)

type storeStateUpdater interface {
	loadedIndex(ctx countlog.Context, indexedSegments []*indexSegment, indexingSegment *indexSegment) error
	movedBlockIntoIndex(ctx countlog.Context, indexingSegment *indexSegment) error
	rotatedIndex(ctx countlog.Context, indexedSegment *indexSegment, indexingSegment *indexSegment) error
	removedIndex(ctx countlog.Context, indexedSegments []*indexSegment) error
}

type storeState struct {
	currentVersion   unsafe.Pointer // pointer to storeVersion, writer use atomic to notify readers
	tailOffset       uint64         // offset, writer use atomic to notify readers
	blockManager     blockManager
	slotIndexManager slotIndexManager
}

type storeVersion struct {
	indexedSegments []*indexSegment
	indexingSegment *indexSegment
	chunks          []*chunk
}

func (version *storeVersion) HeadOffset() Offset {
	if len(version.indexedSegments) != 0 {
		return version.indexedSegments[0].headOffset
	}
	return version.indexingSegment.headOffset
}

func (store *storeState) latest() *storeVersion {
	for {
		version := (*storeVersion)(atomic.LoadPointer(&store.currentVersion))
		return version
	}
}

func (store *storeState) getTailOffset() Offset {
	return Offset(atomic.LoadUint64(&store.tailOffset))
}

func (store *storeState) lock(reader interface{}, lockedFrom Offset) {
	store.blockManager.lock(reader, lockedFrom)
	store.slotIndexManager.lock(reader, lockedFrom)
}

func (store *storeState) unlock(reader interface{}) {
	store.blockManager.unlock(reader)
	store.slotIndexManager.unlock(reader)
}
