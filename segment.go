package lstore

type segmentType uint8

type segmentHeader struct {
	segmentType segmentType
	headOffset  Offset
}

const segmentTypeRaw segmentType = 1
const segmentTypeIndex segmentType = 2
