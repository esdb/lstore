package lstore

import (
	"github.com/v2pro/plz/countlog"
	"os"
	"unsafe"
	"sync/atomic"
)

func (writer *writer) updateCurrentVersion(newVersion *storeVersion) {
	if newVersion == nil {
		return
	}
	atomic.StorePointer(&writer.state.currentVersion, unsafe.Pointer(newVersion))
	writer.currentVersion = newVersion
}

func (writer *writer) incrementTailOffset() {
	writer.tailEntriesCount += 1
	atomic.StoreUint64(&writer.state.tailOffset, writer.state.tailOffset+1)
}

func (writer *writer) setTailOffset(tailOffset Offset) {
	atomic.StoreUint64(&writer.state.tailOffset, uint64(tailOffset))
}

func (writer *writer) loadedIndex(
	ctx countlog.Context, indexedSegments []*indexSegment, indexingSegment *indexSegment) error {
	chunks, err := writer.load(ctx, indexingSegment.tailOffset)
	if err != nil {
		return err
	}
	newVersion := &storeVersion{
		indexedSegments: indexedSegments,
		indexingSegment: indexingSegment,
		chunks:          chunks,
	}
	writer.updateCurrentVersion(newVersion)
	return nil
}

// movedBlockIntoIndex should only be used by indexer
func (writer *writer) movedBlockIntoIndex(
	ctx countlog.Context, indexingSegment *indexSegment) error {
	resultChan := make(chan error)
	writer.asyncExecute(ctx, func(ctx countlog.Context) {
		oldVersion := writer.currentVersion
		newVersion := &storeVersion{
			indexedSegments: append([]*indexSegment(nil), oldVersion.indexedSegments...),
			indexingSegment: indexingSegment,
			chunks:          append([]*chunk(nil), oldVersion.chunks...),
		}
		firstRawChunk := *newVersion.chunks[0] // copy the first raw chunk
		firstRawChunk.headSlot += 4
		if firstRawChunk.headSlot == 64 {
			newVersion.chunks = newVersion.chunks[1:]
		} else {
			newVersion.chunks[0] = &firstRawChunk
		}
		removedRawSegmentsCount := 0
		for i, rawSegment := range writer.rawSegments {
			if rawSegment.headOffset <= indexingSegment.tailOffset {
				removedRawSegmentsCount = i
			} else {
				break
			}
		}
		removedRawSegments := writer.rawSegments[:removedRawSegmentsCount]
		writer.rawSegments = writer.rawSegments[removedRawSegmentsCount:]
		for _, removedRawSegment := range removedRawSegments {
			err := os.Remove(removedRawSegment.path)
			ctx.TraceCall("callee!os.Remove", err,
				"path", removedRawSegment.path)
		}
		writer.updateCurrentVersion(newVersion)
		resultChan <- nil
		countlog.Debug("event!writer.movedBlockIntoIndex",
			"firstRawChunk.headOffset", firstRawChunk.headOffset,
			"firstRawChunk.headSlot", firstRawChunk.headSlot,
			"chunksCount", len(newVersion.chunks),
			"lastChunkTailOffset", newVersion.chunks[len(newVersion.chunks)-1].tailOffset,
			"indexingSegment.headOffset", indexingSegment.headOffset,
			"indexingSegment.tailOffset", indexingSegment.tailOffset,
			"removedRawSegmentsCount", removedRawSegmentsCount)
		return
	})
	return <-resultChan
}

// rotatedIndex should only be used by indexer
func (writer *writer) rotatedIndex(
	ctx countlog.Context, indexedSegment *indexSegment, indexingSegment *indexSegment) error {
	resultChan := make(chan error)
	writer.asyncExecute(ctx, func(ctx countlog.Context) {
		oldVersion := writer.currentVersion
		newVersion := &storeVersion{
			indexedSegments: append(oldVersion.indexedSegments, indexedSegment),
			indexingSegment: indexingSegment,
			chunks:          oldVersion.chunks,
		}
		writer.updateCurrentVersion(newVersion)
		resultChan <- nil
		return
	})
	countlog.Debug("event!writer.rotated index",
		"indexingSegment.headOffset", indexingSegment.headOffset)
	return <-resultChan
}

// removedIndex should only be used by indexer
func (writer *writer) removedIndex(
	ctx countlog.Context, indexedSegments []*indexSegment) error {
	resultChan := make(chan error)
	writer.asyncExecute(ctx, func(ctx countlog.Context) {
		oldVersion := writer.currentVersion
		newVersion := &storeVersion{
			indexedSegments: indexedSegments,
			indexingSegment: oldVersion.indexingSegment,
			chunks:          oldVersion.chunks,
		}
		writer.updateCurrentVersion(newVersion)
		resultChan <- nil
		return
	})
	countlog.Debug("event!writer.removed index")
	return <-resultChan
}
