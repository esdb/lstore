package lstore

import (
	"fmt"
	"github.com/esdb/pbloom"
)

type IndexingStrategyConfig struct {
	BloomFilterIndexedIntColumns  []int
	BloomFilterIndexedBlobColumns []int
	MinMaxIndexedColumns          []int
}

type bloomFilterIndexedColumn [2]int

func (col bloomFilterIndexedColumn) IndexedColumn() int {
	return col[0]
}

func (col bloomFilterIndexedColumn) SourceColumn() int {
	return col[1]
}

type IndexingStrategy struct {
	tinyHashingStrategy           *pbloom.HashingStrategy // used for raw segments
	smallHashingStrategy          *pbloom.HashingStrategy
	mediumHashingStrategy         *pbloom.HashingStrategy
	largeHashingStrategy          *pbloom.HashingStrategy
	bloomFilterIndexedIntColumns  []bloomFilterIndexedColumn
	bloomFilterIndexedBlobColumns []bloomFilterIndexedColumn
	minMaxIndexedColumns          []int
}

func NewIndexingStrategy(config IndexingStrategyConfig) *IndexingStrategy {
	bloomFilterIndexedBlobColumns := make([]bloomFilterIndexedColumn, len(config.BloomFilterIndexedBlobColumns))
	for i := 0; i < len(bloomFilterIndexedBlobColumns); i++ {
		bloomFilterIndexedBlobColumns[i] = bloomFilterIndexedColumn{
			i,
			config.BloomFilterIndexedBlobColumns[i]}
	}
	bloomFilterIndexedIntColumns := make([]bloomFilterIndexedColumn, len(config.BloomFilterIndexedIntColumns))
	for i := 0; i < len(bloomFilterIndexedIntColumns); i++ {
		bloomFilterIndexedIntColumns[i] = bloomFilterIndexedColumn{
			len(bloomFilterIndexedBlobColumns) + i,
			config.BloomFilterIndexedIntColumns[i]}
	}
	largeHashingStrategy := pbloom.NewHashingStrategy(
		pbloom.HasherFnv, 10050663, pbloom.BatchPutLocationsPerElement)
	mediumHashingStrategy := pbloom.NewHashingStrategy(
		pbloom.HasherFnv, 157042, pbloom.BatchPutLocationsPerElement)
	smallHashingStrategy := pbloom.NewHashingStrategy(
		pbloom.HasherFnv, 2454, pbloom.BatchPutLocationsPerElement)
	tinyHashingStrategy := pbloom.NewHashingStrategy(
		pbloom.HasherFnv, 512, 4)
	return &IndexingStrategy{
		largeHashingStrategy:          largeHashingStrategy,
		mediumHashingStrategy:         mediumHashingStrategy,
		smallHashingStrategy:          smallHashingStrategy,
		tinyHashingStrategy:           tinyHashingStrategy,
		bloomFilterIndexedIntColumns:  bloomFilterIndexedIntColumns,
		bloomFilterIndexedBlobColumns: bloomFilterIndexedBlobColumns,
		minMaxIndexedColumns:          config.MinMaxIndexedColumns,
	}
}

func (strategy *IndexingStrategy) bloomFilterIndexedColumnsCount() int {
	return len(strategy.bloomFilterIndexedIntColumns) + len(strategy.bloomFilterIndexedBlobColumns)
}

func (strategy *IndexingStrategy) lookupBlobColumn(sourceColumn int) int {
	for _, c := range strategy.bloomFilterIndexedBlobColumns {
		if c.SourceColumn() == sourceColumn {
			return c.IndexedColumn()
		}
	}
	panic(fmt.Sprintf("blob column not indexed: %d", sourceColumn))
}

func (strategy *IndexingStrategy) lookupIntColumn(sourceColumn int) int {
	for _, c := range strategy.bloomFilterIndexedIntColumns {
		if c.SourceColumn() == sourceColumn {
			return c.IndexedColumn()
		}
	}
	panic(fmt.Sprintf("int column not indexed: %d", sourceColumn))
}

func (strategy *IndexingStrategy) hashingStrategy(level level) *pbloom.HashingStrategy {
	switch level {
	case 0:
		return strategy.smallHashingStrategy
	case 1:
		return strategy.mediumHashingStrategy
	default:
		return strategy.largeHashingStrategy
	}
}