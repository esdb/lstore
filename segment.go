package lstore

type segmentType uint8
type segmentHeader struct {
	segmentType segmentType
	startOffset Offset
}

// 1 tail / multiple raw
const segmentTypeRowBased segmentType = 1

// 1 root indexed / 8 indexing
const segmentTypeIndexing segmentType = 2
const segmentTypeIndexed segmentType = 3
