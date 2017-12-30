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
	})
	return mgr
}

func Test_write_slot_index_to_file_head(t *testing.T) {
	should := require.New(t)
	mgr := testSlotIndexManager(30)
	defer mgr.Close()
	size, err := mgr.writeSlotIndex(0, &slotIndex{children: []uint64{1, 2, 3}})
	should.Nil(err)
	should.True(size > 0)
	mgr.indexCache.Purge()
	idx, err := mgr.readSlotIndex(0)
	should.Nil(err)
	should.Equal([]uint64{1, 2, 3}, idx.children)
}
