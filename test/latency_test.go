package test

import (
	"testing"
	"github.com/stretchr/testify/require"
	"runtime"
	"github.com/esdb/lstore"
	"github.com/rs/xid"
	"fmt"
	"math/rand"
)

func Test_write_1_million(t *testing.T) {
	runtime.GOMAXPROCS(4)
	should := require.New(t)
	cfg := &lstore.Config{}
	cfg.BloomFilterIndexedBlobColumns = []int{0}
	store := testStore(cfg)
	var target lstore.Blob
	for j := 0; j < 10000; j++ {
		for i := 0; i < 100; i++ {
			value := lstore.Blob(xid.New().String())
			if j == 0 && i == 50 {
				fmt.Println(value)
				target = value
			}
			store.Write(ctx, blobEntry(value))
		}
		go store.UpdateIndex(ctx)
	}
	reader, err := store.NewReader(ctx)
	should.NoError(err)
	collector := &lstore.RowsCollector{}
	reader.SearchForward(ctx, &lstore.SearchRequest{
		0, store.NewBlobValueFilter(0, target), collector,
	})
	fmt.Println(collector.Rows)
}

func Test_search(t *testing.T) {
	should := require.New(t)
	cfg := &lstore.Config{}
	cfg.BloomFilterIndexedBlobColumns = []int{0}
	cfg.Directory = "/run/store"
	store, err := lstore.New(ctx, cfg)
	if err != nil {
		panic(err)
	}
	reader, err := store.NewReader(ctx)
	should.NoError(err)
	collector := &lstore.RowsCollector{}
	reader.SearchForward(ctx, &lstore.SearchRequest{
		Callback: &assertContinuous{cb: collector},
	})
	fmt.Println(len(collector.Rows))
	fmt.Println(collector.Rows[899487].BlobValues[0])
}

func Benchmark_search(b *testing.B) {
	cfg := &lstore.Config{}
	cfg.BloomFilterIndexedBlobColumns = []int{0}
	cfg.Directory = "/run/store"
	store, err := lstore.New(ctx, cfg)
	if err != nil {
		panic(err)
	}
	reader, err := store.NewReader(ctx)
	if err != nil {
		b.Error(err)
	}
	collector := &lstore.RowsCollector{}
	reader.SearchForward(ctx, &lstore.SearchRequest{
		0, nil, &assertContinuous{cb: collector},
	})
	fmt.Println(len(collector.Rows))
	fmt.Println(collector.Rows[899487].BlobValues[0])
	rows := collector.Rows
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		collector := &lstore.RowsCollector{LimitSize: 1}
		target := rows[rand.Int31n(1000000)].BlobValues[0]
		reader.SearchForward(ctx, &lstore.SearchRequest{
			0, store.NewBlobValueFilter(0, target), collector,
		})
	}
}
