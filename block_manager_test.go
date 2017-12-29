package lstore

import (
	"testing"
	"github.com/stretchr/testify/require"
	"os"
)

func testBlockManager(blockFileSizeInPowerOfTwo uint8) *blockManager {
	os.RemoveAll("/tmp/block/")
	err := os.Mkdir("/tmp/block/", 0777)
	if err != nil {
		panic(err)
	}
	mgr := newBlockManager("/tmp/block", blockFileSizeInPowerOfTwo)
	return mgr
}

func Test_write_block_to_file_head(t *testing.T) {
	should := require.New(t)
	mgr := testBlockManager(30)
	defer mgr.Close()
	size, err := mgr.writeBlock(0, &block{seqColumn: []RowSeq{1}}, nil)
	should.Nil(err)
	should.True(size > 0)
	blk, err := mgr.readBlock(0)
	should.Nil(err)
	should.Equal([]RowSeq{1}, blk.seqColumn)
}

func Test_block_cache(t *testing.T) {
	should := require.New(t)
	mgr := testBlockManager(30)
	defer mgr.Close()
	size, err := mgr.writeBlock(0, &block{seqColumn: []RowSeq{1}}, nil)
	should.Nil(err)
	should.True(size > 0)
	blk, err := mgr.readBlock(0)
	should.Nil(err)
	should.Equal([]RowSeq{1}, blk.seqColumn)
	blk, err = mgr.readBlock(0)
	should.Nil(err)
	should.Equal([]RowSeq{1}, blk.seqColumn)
}

func Test_write_block_to_file_body(t *testing.T) {
	should := require.New(t)
	mgr := testBlockManager(14)
	defer mgr.Close()
	size, err := mgr.writeBlock(2 << 14 + 777, &block{seqColumn: []RowSeq{1}}, nil)
	should.Nil(err)
	should.True(size > 0)
	blk, err := mgr.readBlock(2 << 14 + 777)
	should.Nil(err)
	should.Equal([]RowSeq{1}, blk.seqColumn)
}

func Test_write_block_to_file_tail_cutting_off_header(t *testing.T) {
	should := require.New(t)
	mgr := testBlockManager(3)
	defer mgr.Close()
	size, err := mgr.writeBlock(0, &block{seqColumn: []RowSeq{1}}, nil)
	should.Nil(err)
	should.True(size > 0)
	blk, err := mgr.readBlock(0)
	should.Nil(err)
	should.Equal([]RowSeq{1}, blk.seqColumn)
}
