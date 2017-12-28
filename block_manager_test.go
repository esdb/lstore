package lstore

import (
	"testing"
	"github.com/stretchr/testify/require"
)

func Test_write_block_to_file_head(t *testing.T) {
	should := require.New(t)
	mgr := newBlockManager("/tmp", 30)
	size, err := mgr.writeBlock(0, &block{})
	should.Nil(err)
	should.True(size > 0)
}
