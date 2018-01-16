package lstore

import (
	"path"
	"fmt"
)

const TailSegmentFileName = "tail.segment"
const TailSegmentTmpFileName = "tail.segment.tmp"

type writerConfig struct {
	RawSegmentDirectory      string
	WriterCommandQueueLength int
	RawSegmentMaxSizeInBytes int64
	ChunkMaxEntriesCount     int
}

func (conf *writerConfig) RawSegmentPath(tailOffset Offset) string {
	return path.Join(conf.RawSegmentDirectory, fmt.Sprintf("raw-%d.segment", tailOffset))
}

func (conf *writerConfig) TailSegmentPath() string {
	return path.Join(conf.RawSegmentDirectory, TailSegmentFileName)
}

func (conf *writerConfig) TailSegmentTmpPath() string {
	return path.Join(conf.RawSegmentDirectory, TailSegmentTmpFileName)
}
