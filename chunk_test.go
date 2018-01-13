package lstore

import (
	"testing"
	"github.com/stretchr/testify/require"
	"github.com/esdb/biter"
	"fmt"
)

func Test_raw_chunk_with_1(t *testing.T) {
	should := require.New(t)
	strategy := NewIndexingStrategy(IndexingStrategyConfig{
		BloomFilterIndexedBlobColumns: []int{0},
	})
	index := newChunk(strategy, 0)
	should.Equal(biter.Slot(1), index.tailSlot)
	should.Equal(biter.Slot(0), index.children[0].tailSlot)
	entry1 := blobEntry("hello")
	index.add(entry1)
	should.Equal(biter.Slot(1), index.tailSlot)
	should.Equal(biter.Slot(1), index.children[0].tailSlot)
	collector := &RowsCollector{}
	index.searchForward(ctx, &SearchRequest{
		0, dummyFilterInstance, collector,
	})
	should.Equal(1, len(collector.Rows))
}

func Test_raw_chunk_with_64(t *testing.T) {
	should := require.New(t)
	strategy := NewIndexingStrategy(IndexingStrategyConfig{
		BloomFilterIndexedBlobColumns: []int{0},
	})
	index := newChunk(strategy, 0)
	should.Equal(biter.Slot(1), index.tailSlot)
	should.Equal(biter.Slot(0), index.children[0].tailSlot)
	for i := 0; i < 64; i++ {
		index.add(blobEntry("hello"))
	}
	should.Equal(biter.Slot(2), index.tailSlot)
	should.Equal(biter.Slot(64), index.children[0].tailSlot)
	should.Equal(biter.Slot(0), index.children[1].tailSlot)
	collector := &RowsCollector{}
	index.searchForward(ctx, &SearchRequest{
		0, dummyFilterInstance, collector,
	})
	should.Equal(64, len(collector.Rows))
}

func Test_raw_chunk_with_65(t *testing.T) {
	should := require.New(t)
	strategy := NewIndexingStrategy(IndexingStrategyConfig{
		BloomFilterIndexedBlobColumns: []int{0},
	})
	index := newChunk(strategy, 0)
	should.Equal(biter.Slot(1), index.tailSlot)
	should.Equal(biter.Slot(0), index.children[0].tailSlot)
	for i := 0; i < 65; i++ {
		index.add(blobEntry("hello"))
	}
	should.Equal(biter.Slot(2), index.tailSlot)
	should.Equal(biter.Slot(64), index.children[0].tailSlot)
	should.Equal(biter.Slot(1), index.children[1].tailSlot)
	collector := &RowsCollector{}
	index.searchForward(ctx, &SearchRequest{
		0, dummyFilterInstance, collector,
	})
	should.Equal(65, len(collector.Rows))
}

func Test_raw_chunk_with_4096(t *testing.T) {
	should := require.New(t)
	strategy := NewIndexingStrategy(IndexingStrategyConfig{
		BloomFilterIndexedBlobColumns: []int{0},
	})
	index := newChunk(strategy, 0)
	should.Equal(biter.Slot(1), index.tailSlot)
	should.Equal(biter.Slot(0), index.children[0].tailSlot)
	for i := 0; i < 4095; i++ {
		should.False(index.add(blobEntry("hello")))
	}
	should.True(index.add(blobEntry("hello")))
	should.Equal(biter.Slot(64), index.tailSlot)
	should.Equal(biter.Slot(64), index.children[0].tailSlot)
	should.Equal(biter.Slot(64), index.children[63].tailSlot)
	collector := &RowsCollector{}
	index.searchForward(ctx, &SearchRequest{
		0, dummyFilterInstance, collector,
	})
	should.Equal(4096, len(collector.Rows))
}

func Test_search_raw_chunk(t *testing.T) {
	should := require.New(t)
	strategy := NewIndexingStrategy(IndexingStrategyConfig{
		BloomFilterIndexedBlobColumns: []int{0},
	})
	index := newChunk(strategy, 0)
	for i := 0; i < 4096; i++ {
		entry := blobEntry(Blob(fmt.Sprintf("hello%v", i)))
		index.add(entry)
	}
	collector := &RowsCollector{}
	index.searchForward(ctx, &SearchRequest{
		0, strategy.NewBlobValueFilter(0, Blob("hello4003")), collector,
	})
	should.Equal(1, len(collector.Rows))
	collector = &RowsCollector{}
	index.searchForward(ctx, &SearchRequest{
		96, dummyFilterInstance, collector,
	})
	should.Equal(4000, len(collector.Rows))
}

func Benchmark_raw_chunk(b *testing.B) {
	strategy := NewIndexingStrategy(IndexingStrategyConfig{
		BloomFilterIndexedBlobColumns: []int{0},
	})
	index := newChunk(strategy, 0)
	for i := 0; i < 4096; i++ {
		entry := blobEntry(Blob(fmt.Sprintf("hello%v", i)))
		index.add(entry)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		collector := &RowsCollector{}
		index.searchForward(ctx, &SearchRequest{
			0, strategy.NewBlobValueFilter(0, Blob("hello4003")), collector,
		})
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
