package lstore

type segmentType uint8
type segmentHeader struct {
	segmentType segmentType
	startOffset Offset
}

const segmentTypeTail segmentType = 1
const segmentTypeHead segmentType = 2
