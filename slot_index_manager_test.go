package lstore

import (
	"testing"
	"github.com/stretchr/testify/require"
	"os"
	"github.com/esdb/biter"
)

func testSlotIndexManager(blockFileSizeInPowerOfTwo uint8) *mmapSlotIndexManager {
	os.RemoveAll("/tmp/index/")
	err := os.Mkdir("/tmp/index/", 0777)
	if err != nil {
		panic(err)
	}
	mgr := newSlotIndexManager(&slotIndexManagerConfig{
		IndexDirectory:            "/tmp/index",
		IndexFileSizeInPowerOfTwo: blockFileSizeInPowerOfTwo,
	}, NewIndexingStrategy(IndexingStrategyConfig{
		BloomFilterIndexedBlobColumns: []int{0},
	}))
	return mgr
}

func Test_write_slot_index_to_file_head(t *testing.T) {
	should := require.New(t)
	mgr := testSlotIndexManager(30)
	defer mgr.Close()
	seq, tailSeq, slotIndex0, err := mgr.newSlotIndex(0, level0)
	should.Equal(slotIndexSeq(0), seq)
	should.Nil(err)
	seq, tailSeq, slotIndex1, err := mgr.newSlotIndex(tailSeq, level1)
	should.Equal(slotIndexSeq(0x4f10), seq)
	should.Nil(err)
	seq, tailSeq, slotIndex2, err := mgr.newSlotIndex(tailSeq, level2)
	should.Equal(slotIndexSeq(0x137d00), seq)
	should.Nil(err)
	slotIndex0.children[0] = 123
	slotIndex1.children[0] = 123
	slotIndex2.children[0] = 123
	slotIndex2.setTailSlot(100)
	idx, err := mgr.mapWritableSlotIndex(0, level0)
	should.Nil(err)
	should.Equal(uint64(123), idx.children[0])
	should.Equal(biter.Bits(0), idx.pbfs[0][0])
	idx, err = mgr.mapWritableSlotIndex(0x137d00, level2)
	should.Nil(err)
	should.Equal(uint64(123), idx.children[0])
	should.Equal(biter.Bits(0), idx.pbfs[0][0])
	should.Equal(biter.Slot(100), *idx.tailSlot)
}
