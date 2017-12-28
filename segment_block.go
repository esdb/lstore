package lstore

type intColumn []int64
type blobColumn []Blob
type blobHashColumn []uint64

type block struct {
	seqColumn       []RowSeq
	intColumns      []intColumn
	blobHashColumns []blobHashColumn
	blobColumns     []blobColumn
}

type blockSegment struct {
	SegmentHeader
	blockSeq BlockSeq
	block    *block // the in memory cache load on demand
}

func (segment *blockSegment) search(reader *Reader, startSeq RowSeq, filters []Filter, collector []Row) ([]Row, error) {
	blk := segment.block
	mask := make([]bool, len(blk.seqColumn))
	for i, seq := range blk.seqColumn {
		if seq >= startSeq {
			mask[i] = true
		}
	}
	for _, filter := range filters {
		filter.updateMask(blk, mask)
	}
	var rows []Row
	for i, matches := range mask {
		if !matches {
			continue
		}
		intColumnsCount := len(blk.intColumns)
		intValues := make([]int64, intColumnsCount)
		for j := 0; j < intColumnsCount; j++ {
			intValues[j] = blk.intColumns[j][i]
		}
		blobColumnsCount := len(blk.blobColumns)
		blobValues := make([]Blob, blobColumnsCount)
		for j := 0; j < blobColumnsCount; j++ {
			blobValues[j] = blk.blobColumns[j][i]
		}
		rows = append(rows, Row{Seq: blk.seqColumn[i], Entry: &Entry{
			EntryType: EntryTypeData, IntValues: intValues, BlobValues: blobValues}})
	}
	return rows, nil
}
