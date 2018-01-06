package lstore

import (
	"testing"
	"github.com/stretchr/testify/require"
	"fmt"
)

func Test_build_raw_segment_index(t *testing.T) {
	should := require.New(t)
	strategy := NewIndexingStrategy(IndexingStrategyConfig{
		BloomFilterIndexedBlobColumns: []int{0},
	})
	index := newRawSegmentIndex(strategy, 0)
	entry1 := blobEntry("hello")
	index.add(entry1)
	collector := &RowsCollector{}
	index.searchForward(ctx, 0, dummyFilterInstance, collector)
	should.Equal(1, len(collector.Rows))
}

func Test_search_raw_segment_index(t *testing.T) {
	should := require.New(t)
	strategy := NewIndexingStrategy(IndexingStrategyConfig{
		BloomFilterIndexedBlobColumns: []int{0},
	})
	index := newRawSegmentIndex(strategy, 0)
	var entries []*Entry
	for i := 0; i < 4096; i++ {
		entry := blobEntry(Blob(fmt.Sprintf("hello%v", i)))
		entries = append(entries, entry)
		index.add(entry)
	}
	collector := &RowsCollector{}
	index.searchForward(ctx, 0, strategy.NewBlobValueFilter(0, Blob("hello4003")), collector)
	should.Equal(1, len(collector.Rows))
}

func Benchmark_raw_segment_index(b *testing.B) {
	strategy := NewIndexingStrategy(IndexingStrategyConfig{
		BloomFilterIndexedBlobColumns: []int{0},
	})
	index := newRawSegmentIndex(strategy, 0)
	for i := 0; i < 4096; i++ {
		entry := blobEntry(Blob(fmt.Sprintf("hello%v", i)))
		index.add(entry)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		collector := &RowsCollector{}
		index.searchForward(ctx, 0, strategy.NewBlobValueFilter(0, Blob("hello4003")), collector)
	}
}

func Benchmark_linear_search(b *testing.B) {
	var entries []*Entry
	for i := 0; i < 4096; i++ {
		entry := blobEntry(Blob(fmt.Sprintf("hello%v", i)))
		entries = append(entries, entry)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		collector := &RowsCollector{}
		for i, entry := range entries {
			if entry.BlobValues[0] == "hello4003" {
				collector.HandleRow(Offset(i), entry)
			}
		}
	}
}
