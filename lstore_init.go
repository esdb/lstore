package lstore

import (
	"path"
	"fmt"
	"os"
	"github.com/v2pro/plz/countlog"
)

func loadInitialVersion(config *Config) (*StoreVersion, error) {
	version := StoreVersion{config: *config}.edit()
	if err := loadIndexedSegments(config, version); err != nil {
		return nil, err
	}
	if err := loadTailAndRawSegments(config, version); err != nil {
		return nil, err
	}
	return version.seal(), nil
}

func loadTailAndRawSegments(config *Config, version *EditingStoreVersion) error {
	tailSegment, err := openTailSegment(config.TailSegmentPath(), config.TailSegmentMaxSize, 0)
	if err != nil {
		return err
	}
	var reversedRawSegments []*rawSegment
	startSeq := tailSegment.startSeq
	tailSeq := RowSeq(0)
	if version.rootIndexedSegment != nil {
		tailSeq = version.rootIndexedSegment.tailSeq
	}
	for startSeq != tailSeq {
		prev := path.Join(config.Directory, fmt.Sprintf("%d.segment", startSeq))
		rawSegment, err := openRawSegment(prev)
		if err != nil {
			countlog.Error("event!lstore.failed to open raw segment",
				"err", err, "path", prev, "tailSeq", tailSeq,
					"rootIndexedSegmentIsNil", version.rootIndexedSegment==nil)
			return err
		}
		reversedRawSegments = append(reversedRawSegments, rawSegment)
		startSeq = rawSegment.startSeq
	}
	rawSegments := make([]*rawSegment, len(reversedRawSegments))
	for i := 0; i < len(reversedRawSegments); i++ {
		rawSegments[i] = reversedRawSegments[len(reversedRawSegments)-i-1]
	}
	version.tailSegment = tailSegment
	version.rawSegments = rawSegments
	return nil
}

func loadIndexedSegments(config *Config, version *EditingStoreVersion) error {
	segmentPath := config.RootIndexedSegmentPath()
	rootIndexedSegment, err := openRootIndexedSegment(segmentPath)
	if os.IsNotExist(err) {
		rootIndexedSegment = nil
	} else if err != nil {
		countlog.Error("event!lstore.failed to load rootIndexedSegment",
			"segmentPath", segmentPath, "err", err)
		return err
	}
	version.rootIndexedSegment = rootIndexedSegment
	return nil
}
