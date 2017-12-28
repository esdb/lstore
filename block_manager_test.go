package lstore

import (
	"testing"
	"github.com/stretchr/testify/require"
	"os"
)

func Test_write_block_to_file_head(t *testing.T) {
	should := require.New(t)
	os.RemoveAll("/tmp/bock")
	os.Mkdir("/tmp/block", 0777)
	mgr := newBlockManager("/tmp/block", 30)
	defer mgr.Close()
	size, err := mgr.writeBlock(0, &block{seqColumn: []RowSeq{1}})
	should.Nil(err)
	should.True(size > 0)
	blk, err := mgr.readBlock(0)
	should.Nil(err)
	should.Equal([]RowSeq{1}, blk.seqColumn)
}
