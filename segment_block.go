package lstore

import (
	"github.com/spaolacci/murmur3"
	"unsafe"
)

type intColumn []int64
type blobColumn []Blob
type blobHashColumn []uint32

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

func newBlock(rows []Row) block {
	rowsCount := len(rows)
	seqColumn := make([]RowSeq, rowsCount)
	intColumnsCount := len(rows[0].IntValues)
	intColumns := make([]intColumn, intColumnsCount)
	blobColumnsCount := len(rows[0].BlobValues)
	blobColumns := make([]blobColumn, blobColumnsCount)
	blobHashColumns := make([]blobHashColumn, blobColumnsCount)
	for i := 0; i < intColumnsCount; i++ {
		intColumns[i] = make(intColumn, rowsCount)
	}
	for i := 0; i < blobColumnsCount; i++ {
		blobColumns[i] = make(blobColumn, rowsCount)
		blobHashColumns[i] = make(blobHashColumn, rowsCount)
	}
	for i, row := range rows {
		seqColumn[i] = row.Seq
		for j, intValue := range row.IntValues {
			intColumns[j][i] = intValue
		}
		hasher := murmur3.New32()
		for j, blobValue := range row.BlobValues {
			blobColumns[j][i] = blobValue
			asSlice := *(*[]byte)(unsafe.Pointer(&blobValue))
			hasher.Reset()
			hasher.Write(asSlice)
			blobHashColumns[j][i] = hasher.Sum32()
		}
	}
	return block{
		seqColumn:       seqColumn,
		intColumns:      intColumns,
		blobColumns:     blobColumns,
		blobHashColumns: blobHashColumns,
	}
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
