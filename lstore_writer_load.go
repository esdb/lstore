package lstore

import (
	"github.com/v2pro/plz/countlog"
	"errors"
)

const firstOffset = 1

func (writer *writer) loadedIndex(
	ctx countlog.Context, indexedSegments []*indexSegment, indexingSegment *indexSegment) error {
	chunks, err := writer.loadChunks(ctx, indexingSegment.tailOffset)
	if err != nil {
		return err
	}
	writer.state.loaded(indexedSegments, indexingSegment, chunks)
	return nil
}

func (writer *writer) loadChunks(ctx countlog.Context, indexingSegmentTailOffset Offset) ([]*chunk, error) {
	tailSegment, entries, err := openTailSegment(
		ctx, writer.cfg.TailSegmentPath(), writer.cfg.RawSegmentMaxSizeInBytes, firstOffset)
	if err != nil {
		return nil, err
	}
	storeTailOffset := tailSegment.headOffset + Offset(tailSegment.tailEntriesCount)
	writer.setTailOffset(storeTailOffset)
	headOffset := tailSegment.headOffset
	var reversedRawSegments []*rawSegment
	offset := tailSegment.headOffset
	for offset > firstOffset && offset > indexingSegmentTailOffset {
		rawSegmentPath := writer.cfg.RawSegmentPath(offset)
		rawSegment, segmentEntries, err := openRawSegment(ctx, rawSegmentPath)
		if err != nil {
			countlog.Error("event!lstore.failed to open raw segment",
				"err", err, "rawSegmentPath", rawSegmentPath,
				"indexingSegmentTailOffset", indexingSegmentTailOffset)
			return nil, err
		}
		reversedRawSegments = append(reversedRawSegments, rawSegment)
		offset = rawSegment.headOffset
		headOffset = rawSegment.headOffset
		entries = append(segmentEntries, entries...)
	}
	rawSegments := make([]*rawSegment, len(reversedRawSegments))
	for i := 0; i < len(reversedRawSegments); i++ {
		rawSegments[i] = reversedRawSegments[len(reversedRawSegments)-i-1]
	}
	if indexingSegmentTailOffset < headOffset {
		return nil, errors.New("found gap between indexing segment tail and raw segment head")
	}
	entries = entries[indexingSegmentTailOffset - headOffset:]
	chunks := newChunks(writer.strategy, indexingSegmentTailOffset)
	chunk := chunks[0]
	for _, entry := range entries {
		if chunk.add(entry) {
			chunk = newChunk(writer.strategy, chunk.tailOffset)
			chunks = append(chunks, chunk)
		}
	}
	writer.appendingChunk = chunks[len(chunks) - 1]
	writer.tailSegment = tailSegment
	writer.rawSegments = rawSegments
	ctx.Info("event!writer.load",
		"firstChunkHeadOffset", chunks[0].headOffset,
		"rawSegmentsCount", len(rawSegments),
		"storeTailOffset", storeTailOffset)
	return chunks, nil
}
