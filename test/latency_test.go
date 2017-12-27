package test

import (
	"testing"
	"context"
	"time"
	"fmt"
	"github.com/esdb/lstore"
)

func Test_write_read_latency(t *testing.T) {
	store := bigTestStore()
	start := time.Now()
	ctx := context.Background()
	resultChan := make(chan lstore.WriteResult, 1024)
	go func() {
		i := 0
		for {
			result := <-resultChan
			if i%(1024*1024) == 0 {
				fmt.Println("seq: ", result.Seq)
			}
			i++
		}
	}()
	for i := 0; i < 1024*1024*4; i++ {
		store.AsyncWrite(ctx, intEntry(int64(i)), resultChan)
	}
	time.Sleep(time.Second * 2)
	end := time.Now()
	fmt.Println(end.Sub(start))
}
