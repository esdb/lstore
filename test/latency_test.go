package test

import (
	"testing"
	"github.com/stretchr/testify/require"
	"runtime"
	"github.com/esdb/lstore"
	"github.com/rs/xid"
	"fmt"
	"context"
)

func Test_write_1_million(t *testing.T) {
	runtime.GOMAXPROCS(4)
	should := require.New(t)
	strategy := lstore.NewIndexingStrategy(lstore.IndexingStrategyConfig{
		BloomFilterIndexedBlobColumns: []int{0},
	})
	store := testStore(lstore.Config{
		IndexingStrategy: strategy,
	})
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
		store.UpdateIndex()
	}
	reader, err := store.NewReader(ctx)
	should.NoError(err)
	collector := &lstore.RowsCollector{}
	reader.SearchForward(ctx, &lstore.SearchRequest{
		0, strategy.NewBlobValueFilter(0, target), collector,
	})
	fmt.Println(collector.Rows)
}

func Test_search(t *testing.T) {
	should := require.New(t)
	strategy := lstore.NewIndexingStrategy(lstore.IndexingStrategyConfig{
		BloomFilterIndexedBlobColumns: []int{0},
	})
	store := &lstore.Store{Config: lstore.Config{
		IndexingStrategy: strategy,
	}}
	store.Directory = "/tmp/store"
	err := store.Start(context.Background())
	if err != nil {
		panic(err)
	}
	reader, err := store.NewReader(ctx)
	should.NoError(err)
	collector := &lstore.RowsCollector{}
	reader.SearchForward(ctx, &lstore.SearchRequest{
		0, nil, collector,
	})
	fmt.Println(len(collector.Rows))
	fmt.Println(collector.Rows[899487].BlobValues[0])
}

func Benchmark_search(b *testing.B) {
	strategy := lstore.NewIndexingStrategy(lstore.IndexingStrategyConfig{
		BloomFilterIndexedBlobColumns: []int{0},
	})
	store := &lstore.Store{Config: lstore.Config{
		IndexingStrategy: strategy,
	}}
	store.Directory = "/tmp/store"
	err := store.Start(context.Background())
	if err != nil {
		panic(err)
	}
	reader, err := store.NewReader(ctx)
	if err != nil {
		b.Error(err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		collector := &lstore.RowsCollector{LimitSize: 1}
		reader.SearchForward(ctx, &lstore.SearchRequest{
			0, strategy.NewBlobValueFilter(0, "b9c3b60t8744tkh6gl6g"), collector,
		})
	}
}

//func Test_write_read_latency(t *testing.T) {
//	runtime.GOMAXPROCS(4)
//	should := require.New(t)
//	store := testStore()
//	start := time.Now()
//	ctx := context.Background()
//	resultChan := make(chan lstore.WriteResult, 1024)
//	go func() {
//		for {
//			result := <-resultChan
//			fmt.Println("write: ", result.Offset, time.Now())
//		}
//	}()
//	go func() {
//		countlog.Info("event!test.search")
//		reader, err := store.NewReader(context.Background())
//		should.Nil(err)
//		startOffset := lstore.Offset(0)
//		for {
//			hasNew, err := reader.Refresh(context.Background())
//			should.Nil(err)
//			if !hasNew {
//				time.Sleep(time.Millisecond)
//				continue
//			}
//			iter := reader.Search(ctx, lstore.SearchRequest{StartOffset: startOffset, LimitSize: 1024 * 1024})
//			for {
//				rows, err := iter()
//				if err == io.EOF {
//					break
//				}
//				if err != nil {
//					panic(err)
//				}
//				for _, row := range rows {
//					fmt.Println("read: ", row.Offset, time.Now())
//				}
//				startOffset = rows[len(rows) - 1].Offset
//			}
//		}
//	}()
//	for i := 0; i < 1024; i++ {
//		store.AsyncWrite(ctx, intEntry(int64(i)), resultChan)
//	}
//	time.Sleep(time.Second * 5)
//	end := time.Now()
//	fmt.Println(end.Sub(start))
//}
