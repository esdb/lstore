package lstore

import (
	"github.com/v2pro/plz/countlog"
)

const firstOffset = 1

func (writer *writer) load(ctx countlog.Context, indexingSegmentTailOffset Offset) ([]*chunk, error) {
	tailChunk, entries, err := openTailSegment(
		ctx, writer.cfg.TailSegmentPath(), writer.cfg.RawSegmentMaxSizeInBytes, firstOffset)
	if err != nil {
		return nil, err
	}
	writer.setTailOffset(tailChunk.headOffset + Offset(tailChunk.tailEntriesCount))
	headOffset := tailChunk.headOffset
	var reversedRawSegments []*rawSegment
	offset := tailChunk.headOffset
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
	chunks := newChunks(writer.strategy, headOffset)
	chunk := chunks[0]
	for _, entry := range entries {
		if chunk.add(entry) {
			chunk = newChunk(writer.strategy, headOffset)
			chunks = append(chunks, chunk)
		}
	}
	writer.tailSegment = tailChunk
	writer.rawSegments = rawSegments
	return chunks, nil
}