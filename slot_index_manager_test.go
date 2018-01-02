package lstore

import (
	"testing"
	"github.com/stretchr/testify/require"
	"os"
)

func testSlotIndexManager(blockFileSizeInPowerOfTwo uint8) *slotIndexManager {
	os.RemoveAll("/tmp/index/")
	err := os.Mkdir("/tmp/index/", 0777)
	if err != nil {
		panic(err)
	}
	mgr := newSlotIndexManager(&slotIndexManagerConfig{
		IndexDirectory:            "/tmp/index",
		IndexFileSizeInPowerOfTwo: blockFileSizeInPowerOfTwo,
	}, NewIndexingStrategy(IndexingStrategyConfig{}))
	return mgr
}

func Test_write_slot_index_to_file_head(t *testing.T) {
	should := require.New(t)
	mgr := testSlotIndexManager(30)
	defer mgr.Close()
	seq, _, slotIndex, err := mgr.newSlotIndex(0, level0)
	should.Equal(slotIndexSeq(0), seq)
	should.Nil(err)
	slotIndex.children[0] = 123
	mgr.flush(0, level0)
	mgr.indexCache.Purge()
	idx, err := mgr.mapWritableSlotIndex(0, level0)
	should.Nil(err)
	should.Equal(uint64(123), idx.children[0])
}
