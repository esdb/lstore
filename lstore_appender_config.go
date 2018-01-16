package lstore

import (
	"path"
	"fmt"
)

const tailSegmentFileName = "tail.segment"
const tailSegmentTmpFileName = "tail.segment.tmp"

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
	return path.Join(conf.RawSegmentDirectory, tailSegmentFileName)
}

func (conf *writerConfig) TailSegmentTmpPath() string {
	return path.Join(conf.RawSegmentDirectory, tailSegmentTmpFileName)
}
