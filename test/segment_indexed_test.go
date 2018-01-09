package test

import (
	"testing"
	"github.com/stretchr/testify/require"
	"github.com/esdb/lstore"
	"strconv"
	"github.com/minio/minio/pkg/x/os"
	"time"
)

func Test_indexed_segment(t *testing.T) {
	should := require.New(t)
	store := testStore(lstore.Config{})
	defer store.Stop(ctx)
	for i := 0; i < 260; i++ {
		blobValue := lstore.Blob(strconv.Itoa(i))
		offset, err := store.Write(ctx, intBlobEntry(int64(i), blobValue))
		should.Nil(err)
		should.Equal(lstore.Offset(i), offset)
	}
	should.Nil(store.UpdateIndex())
	should.Nil(store.RotateIndex())

	reader, err := store.NewReader(ctx)
	should.Nil(err)
	collector := &lstore.RowsCollector{}
	reader.SearchForward(ctx, 0, nil, collector)
	should.Equal(260, len(collector.Rows))
	for _, row := range collector.Rows {
		should.Equal(row.IntValues[0], int64(row.Offset))
	}

	for i := 260; i < 520; i++ {
		blobValue := lstore.Blob(strconv.Itoa(i))
		_, err := store.Write(ctx, intBlobEntry(int64(i), blobValue))
		should.Nil(err)
	}
	should.Nil(store.UpdateIndex())

	hasNew := reader.Refresh(ctx)
	should.True(hasNew)
	collector = &lstore.RowsCollector{}
	reader.SearchForward(ctx, 0, nil, collector)
	should.Equal(520, len(collector.Rows))
	for _, row := range collector.Rows {
		should.Equal(row.IntValues[0], int64(row.Offset))
	}
}

func Test_reopen_indexed_segments(t *testing.T) {
	should := require.New(t)
	store := testStore(lstore.Config{})
	defer store.Stop(ctx)
	for i := 0; i < 260; i++ {
		blobValue := lstore.Blob(strconv.Itoa(i))
		offset, err := store.Write(ctx, intBlobEntry(int64(i), blobValue))
		should.Nil(err)
		should.Equal(lstore.Offset(i), offset)
	}
	should.Nil(store.UpdateIndex())
	should.Nil(store.RotateIndex())

	store = reopenTestStore(store)

	reader, err := store.NewReader(ctx)
	should.Nil(err)
	collector := &lstore.RowsCollector{}
	should.NoError(reader.SearchForward(ctx, 0, nil, collector))
	should.Equal(260, len(collector.Rows))
	for _, row := range collector.Rows {
		should.Equal(row.IntValues[0], int64(row.Offset))
	}

	for i := 260; i < 520; i++ {
		blobValue := lstore.Blob(strconv.Itoa(i))
		_, err := store.Write(ctx, intBlobEntry(int64(i), blobValue))
		should.Nil(err)
	}
	should.Nil(store.UpdateIndex())

	hasNew := reader.Refresh(ctx)
	should.True(hasNew)
	collector = &lstore.RowsCollector{}
	reader.SearchForward(ctx, 0, nil, collector)
	should.Equal(520, len(collector.Rows))
	for _, row := range collector.Rows {
		should.Equal(row.IntValues[0], int64(row.Offset))
	}
}

func Test_remove_indexed_segment(t *testing.T) {
	should := require.New(t)
	config := lstore.Config{}
	config.BlockFileSizeInPowerOfTwo = 14
	store := testStore(config)
	defer store.Stop(ctx)
	for j := 0; j < 4; j++ {
		for i := 0; i < 1024; i++ {
			blobValue := lstore.Blob(strconv.Itoa(i))
			_, err := store.Write(ctx, intBlobEntry(int64(i), blobValue))
			should.Nil(err)
		}
		should.Nil(store.UpdateIndex())
		should.Nil(store.RotateIndex())
	}
	should.Nil(store.Remove(1792))

	reader, err := store.NewReader(ctx)
	should.Nil(err)
	collector := &lstore.RowsCollector{}
	reader.SearchForward(ctx, 0, nil, &assertSearchForward{collector, 0})
	should.Equal(2304, len(collector.Rows))
	should.NoError(reader.Close())
	time.Sleep(time.Second)
	_, err = os.Stat("/tmp/store/block/2")
	should.Error(err)
	_, err = os.Stat("/tmp/store/block/3")
	should.NoError(err)
}
