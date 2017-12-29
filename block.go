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
type blockHashCache []hashColumn

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

func NewIndexingStrategy(config IndexingStrategyConfig) *IndexingStrategy {
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

func newBlock(strategy *IndexingStrategy, rows []Row) (*block, blockHashCache) {
	rowsCount := len(rows)
	seqColumn := make([]RowSeq, rowsCount)
	intColumnsCount := len(rows[0].IntValues)
	intColumns := make([]intColumn, intColumnsCount)
	for i := 0; i < intColumnsCount; i++ {
		intColumns[i] = make(intColumn, rowsCount)
	}
	blobColumnsCount := len(rows[0].BlobValues)
	blobColumns := make([]blobColumn, blobColumnsCount)
	blobHashColumns := make([]blobHashColumn, blobColumnsCount)
	for i := 0; i < blobColumnsCount; i++ {
		blobColumns[i] = make(blobColumn, rowsCount)
		blobHashColumns[i] = make(blobHashColumn, rowsCount)
	}
	blockHashCache := make(blockHashCache, strategy.BloomFilterIndexedColumnsCount())
	for i := 0; i < len(blockHashCache); i++ {
		blockHashCache[i] = make(hashColumn, rowsCount)
	}
	for i, row := range rows {
		seqColumn[i] = row.Seq
		for j, intValue := range row.IntValues {
			intColumns[j][i] = intValue
		}
		for j, blobValue := range row.BlobValues {
			blobColumns[j][i] = blobValue
			asSlice := *(*[]byte)(unsafe.Pointer(&blobValue))
			blobHashColumns[j][i] = strategy.hasher(asSlice).DownCastToUint32()
		}
		for _, bfIndexedColumn := range strategy.bloomFilterIndexedIntColumns {
			sourceValue := row.IntValues[bfIndexedColumn.SourceColumn()]
			asSlice := (*(*[8]byte)(unsafe.Pointer(&sourceValue)))[:]
			blockHashCache[bfIndexedColumn.IndexedColumn()][i] = strategy.hasher(asSlice)
		}
		for _, bfIndexedColumn := range strategy.bloomFilterIndexedBlobColumns {
			sourceValue := row.BlobValues[bfIndexedColumn.SourceColumn()]
			asSlice := *(*[]byte)(unsafe.Pointer(&sourceValue))
			blockHashCache[bfIndexedColumn.IndexedColumn()][i] = strategy.hasher(asSlice)
		}
	}
	return &block{
		seqColumn:       seqColumn,
		intColumns:      intColumns,
		blobColumns:     blobColumns,
		blobHashColumns: blobHashColumns,
	}, blockHashCache
}
