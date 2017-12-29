package lstore

import (
	"unsafe"
	"github.com/esdb/pbloom"
)

type intColumn []int64
type blobColumn []Blob
type blobHashColumn []uint32

type block struct {
	seqColumn   []RowSeq
	intColumns  []intColumn
	blobColumns []blobColumn
	// to speed up blob column linear search
	blobHashColumns []blobHashColumn
}

type hashColumn []pbloom.HashedElement

// to speed up computation of bloom filter
type blockHash []hashColumn

type IndexingStrategyConfig struct {
	Hasher                        pbloom.Hasher
	BloomFilterIndexedIntColumns  []int
	BloomFilterIndexedBlobColumns []int
	MinMaxIndexedColumns          []int
}

type BloomFilterIndexedColumn [2]int

func (col BloomFilterIndexedColumn) IndexedColumn() int {
	return col[0]
}

func (col BloomFilterIndexedColumn) SourceColumn() int {
	return col[1]
}

type IndexingStrategy struct {
	hasher                        pbloom.Hasher
	bloomFilterIndexedIntColumns  []BloomFilterIndexedColumn
	bloomFilterIndexedBlobColumns []BloomFilterIndexedColumn
	minMaxIndexedColumns          []int
}

func NewIndexingStrategy(config *IndexingStrategyConfig) *IndexingStrategy {
	hasher := config.Hasher
	if hasher == nil {
		hasher = pbloom.HasherFnv
	}
	bloomFilterIndexedIntColumns := make([]BloomFilterIndexedColumn, len(config.BloomFilterIndexedIntColumns))
	for i := 0; i < len(bloomFilterIndexedIntColumns); i++ {
		bloomFilterIndexedIntColumns[i] = BloomFilterIndexedColumn{i, config.BloomFilterIndexedIntColumns[i]}
	}
	bloomFilterIndexedBlobColumns := make([]BloomFilterIndexedColumn, len(config.BloomFilterIndexedBlobColumns))
	for i := 0; i < len(bloomFilterIndexedBlobColumns); i++ {
		bloomFilterIndexedBlobColumns[i] = BloomFilterIndexedColumn{
			len(bloomFilterIndexedIntColumns) + i,
			config.BloomFilterIndexedBlobColumns[i]}
	}
	return &IndexingStrategy{
		hasher:                        hasher,
		bloomFilterIndexedIntColumns:  bloomFilterIndexedIntColumns,
		bloomFilterIndexedBlobColumns: bloomFilterIndexedBlobColumns,
		minMaxIndexedColumns:          config.MinMaxIndexedColumns,
	}
}

func (strategy *IndexingStrategy) BloomFilterIndexedColumnsCount() int {
	return len(strategy.bloomFilterIndexedIntColumns) + len(strategy.bloomFilterIndexedBlobColumns)
}

func newBlock(rows []Row) *block {
	rowsCount := len(rows)
	seqColumn := make([]RowSeq, rowsCount)
	intColumnsCount := len(rows[0].IntValues)
	intColumns := make([]intColumn, intColumnsCount)
	for i := 0; i < intColumnsCount; i++ {
		intColumns[i] = make(intColumn, rowsCount)
	}
	blobColumnsCount := len(rows[0].BlobValues)
	blobColumns := make([]blobColumn, blobColumnsCount)
	for i := 0; i < blobColumnsCount; i++ {
		blobColumns[i] = make(blobColumn, rowsCount)
	}
	for i, row := range rows {
		seqColumn[i] = row.Seq
		for j, intValue := range row.IntValues {
			intColumns[j][i] = intValue
		}
		for j, blobValue := range row.BlobValues {
			blobColumns[j][i] = blobValue
		}
	}
	return &block{
		seqColumn:       seqColumn,
		intColumns:      intColumns,
		blobColumns:     blobColumns,
	}
}

func (blk *block) Hash(strategy *IndexingStrategy) blockHash {
	rowsCount := len(blk.seqColumn)
	blockHash := make(blockHash, strategy.BloomFilterIndexedColumnsCount())
	for i := 0; i < len(blockHash); i++ {
		blockHash[i] = make(hashColumn, rowsCount)
	}
	blobHashColumns := make([]blobHashColumn, len(strategy.bloomFilterIndexedBlobColumns))
	for i := 0; i < len(blobHashColumns); i++ {
		blobHashColumns[i] = make(blobHashColumn, rowsCount)
	}
	for _, bfIndexedColumn := range strategy.bloomFilterIndexedIntColumns {
		indexedColumn := bfIndexedColumn.IndexedColumn()
		sourceColumn := bfIndexedColumn.SourceColumn()
		for i, sourceValue := range blk.intColumns[sourceColumn] {
			asSlice := (*(*[8]byte)(unsafe.Pointer(&sourceValue)))[:]
			blockHash[indexedColumn][i] = strategy.hasher(asSlice)
		}
	}
	for j, bfIndexedColumn := range strategy.bloomFilterIndexedBlobColumns {
		indexedColumn := bfIndexedColumn.IndexedColumn()
		sourceColumn := bfIndexedColumn.SourceColumn()
		for i, sourceValue := range blk.blobColumns[sourceColumn] {
			asSlice := *(*[]byte)(unsafe.Pointer(&sourceValue))
			hashed := strategy.hasher(asSlice)
			blockHash[indexedColumn][i] = hashed
			blobHashColumns[j][i] = hashed.DownCastToUint32()
		}
	}
	blk.blobHashColumns = blobHashColumns
	return blockHash
}

func (blk *block) search(reader *Reader, startSeq RowSeq, filters []Filter, collector []Row) ([]Row, error) {
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