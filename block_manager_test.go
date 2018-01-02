package lstore

import (
	"testing"
	"github.com/stretchr/testify/require"
	"os"
)

func testBlockManager(blockFileSizeInPowerOfTwo uint8) *mmapBlockManager {
	os.RemoveAll("/tmp/block/")
	err := os.Mkdir("/tmp/block/", 0777)
	if err != nil {
		panic(err)
	}
	mgr := newBlockManager(&blockManagerConfig{
		BlockDirectory: "/tmp/block",
		BlockFileSizeInPowerOfTwo: blockFileSizeInPowerOfTwo,
	})
	return mgr
}

func Test_write_block_to_file_head(t *testing.T) {
	should := require.New(t)
	mgr := testBlockManager(30)
	defer mgr.Close()
	start, size, err := mgr.writeBlock(0, &block{startOffset: 1})
	should.Equal(blockSeq(0), start)
	should.Nil(err)
	should.True(size > 0)
	mgr.blockCache.Purge()
	blk, err := mgr.readBlock(0)
	should.Nil(err)
	should.Equal(Offset(1), blk.startOffset)
}

func Test_block_cache(t *testing.T) {
	should := require.New(t)
	mgr := testBlockManager(30)
	defer mgr.Close()
	start, size, err := mgr.writeBlock(0, &block{startOffset: 1})
	should.Equal(blockSeq(0), start)
	should.Nil(err)
	should.True(size > 0)
	blk, err := mgr.readBlock(0)
	should.Nil(err)
	should.Equal(Offset(1), blk.startOffset)
	blk, err = mgr.readBlock(0)
	should.Nil(err)
	should.Equal(Offset(1), blk.startOffset)

}

func Test_write_block_to_file_body(t *testing.T) {
	should := require.New(t)
	mgr := testBlockManager(14)
	defer mgr.Close()
	start, size, err := mgr.writeBlock(2 << 14 + 777, &block{startOffset: 1})
	should.Equal(blockSeq(2 << 14 + 777), start)
	should.Nil(err)
	should.True(size > 0)
	mgr.blockCache.Purge()
	blk, err := mgr.readBlock(2 << 14 + 777)
	should.Nil(err)
	should.Equal(Offset(1), blk.startOffset)
}

func Test_write_block_to_file_tail_cutting_off_header(t *testing.T) {
	should := require.New(t)
	mgr := testBlockManager(8)
	defer mgr.Close()
	start, size, err := mgr.writeBlock(253, &block{startOffset: 1})
	should.Equal(blockSeq(256), start)
	should.Nil(err)
	should.True(size > 0)
	mgr.blockCache.Purge()
	blk, err := mgr.readBlock(start)
	should.Nil(err)
	should.Equal(Offset(1), blk.startOffset)
}
