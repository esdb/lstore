package lstore

import (
	"os"
	"testing"
	"github.com/stretchr/testify/require"
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
	writer := mgr.newWriter()
	defer writer.Close()
	reader := mgr.newReader(10, 4)
	defer reader.Close()
	start, size, err := writer.writeBlock(0, &block{startOffset: 1})
	should.Equal(blockSeq(0), start)
	should.Nil(err)
	should.True(size > 0)
	blk, err := reader.readBlock(0)
	should.Nil(err)
	should.Equal(Offset(1), blk.startOffset)
}

func Test_block_cache(t *testing.T) {
	should := require.New(t)
	mgr := testBlockManager(30)
	defer mgr.Close()
	writer := mgr.newWriter()
	defer writer.Close()
	reader := mgr.newReader(10, 4)
	defer reader.Close()
	start, size, err := writer.writeBlock(0, &block{startOffset: 1})
	should.Equal(blockSeq(0), start)
	should.Nil(err)
	should.True(size > 0)
	blk, err := reader.readBlock(0)
	should.Nil(err)
	should.Equal(Offset(1), blk.startOffset)
	blk, err = reader.readBlock(0)
	should.Nil(err)
	should.Equal(Offset(1), blk.startOffset)
}

func Test_write_block_to_file_body(t *testing.T) {
	should := require.New(t)
	mgr := testBlockManager(14)
	defer mgr.Close()
	writer := mgr.newWriter()
	defer writer.Close()
	reader := mgr.newReader(10, 4)
	defer reader.Close()
	start, size, err := writer.writeBlock(2 << 14 + 777, &block{startOffset: 1})
	should.Equal(blockSeq(2 << 14 + 777), start)
	should.Nil(err)
	should.True(size > 0)
	blk, err := reader.readBlock(2 << 14 + 777)
	should.Nil(err)
	should.Equal(Offset(1), blk.startOffset)
}

func Test_write_block_to_file_tail_cutting_off_header(t *testing.T) {
	should := require.New(t)
	mgr := testBlockManager(8)
	defer mgr.Close()
	writer := mgr.newWriter()
	defer writer.Close()
	reader := mgr.newReader(10, 4)
	defer reader.Close()
	start, size, err := writer.writeBlock(253, &block{startOffset: 1})
	should.Equal(blockSeq(256), start)
	should.Nil(err)
	should.True(size > 0)
	blk, err := reader.readBlock(start)
	should.Nil(err)
	should.Equal(Offset(1), blk.startOffset)
}
