package test

import (
	"testing"
	"github.com/stretchr/testify/require"
	"github.com/esdb/lstore"
	"strconv"
	"github.com/minio/minio/pkg/x/os"
	"time"
	"io/ioutil"
	"strings"
)

func Test_indexed_segment(t *testing.T) {
	should := require.New(t)
	cfg := &lstore.Config{}
	cfg.ChunkMaxEntriesCount = 256
	store := testStore(cfg)
	defer store.Stop(ctx)
	for i := 0; i < 260; i++ {
		blobValue := lstore.Blob(strconv.Itoa(i))
		offset, err := store.Write(ctx, intBlobEntry(int64(i), blobValue))
		should.Nil(err)
		should.Equal(lstore.Offset(i + 1), offset)
	}
	should.Nil(store.UpdateIndex(ctx))
	should.Nil(store.RotateIndex(ctx))

	reader, err := store.NewReader(ctx)
	should.Nil(err)
	collector := &lstore.RowsCollector{}
	reader.SearchForward(ctx, &lstore.SearchRequest{
		0, nil, collector,
	})
	should.Equal(260, len(collector.Rows))
	for _, row := range collector.Rows {
		should.Equal(row.IntValues[0] + 1, int64(row.Offset))
	}

	for i := 260; i < 520; i++ {
		blobValue := lstore.Blob(strconv.Itoa(i))
		_, err := store.Write(ctx, intBlobEntry(int64(i), blobValue))
		should.Nil(err)
	}
	should.Nil(store.UpdateIndex(ctx))

	hasNew := reader.Refresh(ctx)
	should.True(hasNew)
	collector = &lstore.RowsCollector{}
	reader.SearchForward(ctx, &lstore.SearchRequest{
		0, nil, collector,
	})
	should.Equal(520, len(collector.Rows))
	for _, row := range collector.Rows {
		should.Equal(row.IntValues[0] + 1, int64(row.Offset))
	}
}

func Test_reopen_indexed_segments(t *testing.T) {
	should := require.New(t)
	cfg := &lstore.Config{}
	cfg.ChunkMaxEntriesCount = 256
	store := testStore(cfg)
	for i := 0; i < 260; i++ {
		blobValue := lstore.Blob(strconv.Itoa(i))
		offset, err := store.Write(ctx, intBlobEntry(int64(i), blobValue))
		should.Nil(err)
		should.Equal(lstore.Offset(i + 1), offset)
	}
	should.Nil(store.UpdateIndex(ctx))
	should.Nil(store.RotateIndex(ctx))

	store = reopenTestStore(store)
	defer store.Stop(ctx)

	reader, err := store.NewReader(ctx)
	should.Nil(err)
	should.Equal(lstore.Offset(261), reader.TailOffset())
	collector := &lstore.RowsCollector{}
	reader.SearchForward(ctx, &lstore.SearchRequest{
		0, nil, collector,
	})
	should.Equal(260, len(collector.Rows))
	for _, row := range collector.Rows {
		should.Equal(row.IntValues[0] + 1, int64(row.Offset))
	}

	for i := 260; i < 520; i++ {
		blobValue := lstore.Blob(strconv.Itoa(i))
		_, err := store.Write(ctx, intBlobEntry(int64(i), blobValue))
		should.Nil(err)
	}
	should.Nil(store.UpdateIndex(ctx))

	hasNew := reader.Refresh(ctx)
	should.True(hasNew)
	collector = &lstore.RowsCollector{}
	reader.SearchForward(ctx, &lstore.SearchRequest{
		0, nil, collector,
	})
	should.Equal(520, len(collector.Rows))
	for _, row := range collector.Rows {
		should.Equal(row.IntValues[0] + 1, int64(row.Offset))
	}
}

func Test_auto_rotate_index(t *testing.T) {
	should := require.New(t)
	cfg := &lstore.Config{}
	cfg.ChunkMaxEntriesCount = 256
	cfg.UpdateIndexInterval = time.Millisecond * 100
	cfg.IndexSegmentMaxEntriesCount = 256 * 64
	store := testStore(cfg)
	for i := 0; i < 256 * 64 * 4; i++ {
		blobValue := lstore.Blob(strconv.Itoa(i))
		offset, err := store.Write(ctx, intBlobEntry(int64(i), blobValue))
		should.Nil(err)
		should.Equal(lstore.Offset(i + 1), offset)
	}
	time.Sleep(time.Millisecond * 200)
	infos, err := ioutil.ReadDir("/run/store")
	should.NoError(err)
	found := false
	for _, info := range infos {
		if strings.HasPrefix(info.Name(), "indexed-") {
			found = true
		}
	}
	should.True(found)
}

func Test_remove_indexed_segment(t *testing.T) {
	should := require.New(t)
	config := &lstore.Config{}
	config.BlockFileSizeInPowerOfTwo = 14
	store := testStore(config)
	defer store.Stop(ctx)
	for j := 0; j < 4; j++ {
		for i := 0; i < 1024; i++ {
			blobValue := lstore.Blob(strconv.Itoa(i))
			_, err := store.Write(ctx, intBlobEntry(int64(i), blobValue))
			should.Nil(err)
		}
		should.Nil(store.UpdateIndex(ctx))
		should.Nil(store.RotateIndex(ctx))
	}
	should.Nil(store.Remove(ctx,1792))

	reader, err := store.NewReader(ctx)
	should.Nil(err)
	collector := &lstore.RowsCollector{}
	reader.SearchForward(ctx, &lstore.SearchRequest{
		0, nil, &assertSearchForward{collector, 0},
	})
	should.Equal(2304, len(collector.Rows))
	should.NoError(reader.Close())
	time.Sleep(time.Second)
	_, err = os.Stat("/tmp/store/block/2")
	should.Error(err)
	_, err = os.Stat("/tmp/store/block/3")
	should.NoError(err)
}
