package test

import (
	"testing"
	"github.com/stretchr/testify/require"
	"github.com/esdb/lstore"
	"strconv"
)

func Test_indexed_segment(t *testing.T) {
	should := require.New(t)
	store := bigTestStore()
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
	reader.SearchForward(ctx, 0, nil,collector)
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

	hasNew, err := reader.Refresh(ctx)
	should.Nil(err)
	should.True(hasNew)
	collector = &lstore.RowsCollector{}
	reader.SearchForward(ctx, 0, nil,collector)
	should.Equal(520, len(collector.Rows))
	for _, row := range collector.Rows {
		should.Equal(row.IntValues[0], int64(row.Offset))
	}
}

func Test_reopen_indexed_segments(t *testing.T) {
	should := require.New(t)
	store := bigTestStore()
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
	reader.SearchForward(ctx, 0, nil,collector)
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

	hasNew, err := reader.Refresh(ctx)
	should.Nil(err)
	should.True(hasNew)
	collector = &lstore.RowsCollector{}
	reader.SearchForward(ctx, 0, nil,collector)
	should.Equal(520, len(collector.Rows))
	for _, row := range collector.Rows {
		should.Equal(row.IntValues[0], int64(row.Offset))
	}
}