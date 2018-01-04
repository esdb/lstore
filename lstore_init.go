package lstore

import (
	"github.com/v2pro/plz/countlog"
)

func (writer *writer) loadInitialVersion(ctx countlog.Context) error {
	version := StoreVersion{}.edit()
	indexedSegment, err := openIndexingSegment(
		ctx, writer.store.IndexingSegmentPath(),
		writer.store.blockManager, writer.store.slotIndexManager)
	ctx.TraceCall("callee!store.openIndexingSegment", err)
	if err != nil {
		return err
	}
	version.indexingSegment = indexedSegment
	err = writer.loadTailAndRawSegments(ctx, version)
	ctx.TraceCall("callee!store.loadTailAndRawSegments", err)
	if err != nil {
		return err
	}
	writer.currentVersion = version.seal()
	return nil
}

func (writer *writer) loadTailAndRawSegments(ctx countlog.Context, version *EditingStoreVersion) error {
	tailSegment, err := openTailSegment(ctx, writer.store.TailSegmentPath(), writer.store.TailSegmentMaxSize, 0)
	if err != nil {
		return err
	}
	var reversedRawSegments []*rawSegment
	startOffset := tailSegment.startOffset
	for startOffset > version.indexingSegment.tailOffset {
		rawSegmentPath := writer.store.RawSegmentPath(startOffset)
		rawSegment, err := openRawSegment(ctx, rawSegmentPath)
		if err != nil {
			countlog.Error("event!lstore.failed to open raw segment",
				"err", err, "rawSegmentPath", rawSegmentPath,
				"headSegmentTailOffset", version.indexingSegment.tailOffset)
			return err
		}
		reversedRawSegments = append(reversedRawSegments, rawSegment)
		startOffset = rawSegment.startOffset
	}
	rawSegments := make([]*rawSegment, len(reversedRawSegments))
	for i := 0; i < len(reversedRawSegments); i++ {
		rawSegments[i] = reversedRawSegments[len(reversedRawSegments)-i-1]
	}
	writer.tailSegment = tailSegment
	version.tailSegment = tailSegment.rawSegment
	version.rawSegments = rawSegments
	return nil
}
