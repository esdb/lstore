package lstore

import (
	"testing"
	"github.com/stretchr/testify/require"
	"fmt"
)

func realEditingHead() (*editingHead, *blockManager, *slotIndexManager) {
	strategy := NewIndexingStrategy(IndexingStrategyConfig{
		BloomFilterIndexedBlobColumns: []int{0},
	})
	levels := make([]*slotIndex, levelsCount)
	for i := level0; i < level(len(levels)); i++ {
		levels[i] = newSlotIndex(strategy, i)
	}
	blockManager := newBlockManager(&blockManagerConfig{})
	slotIndexManager := newSlotIndexManager(&slotIndexManagerConfig{}, strategy)
	return &editingHead{
		strategy: strategy,
		headSegmentVersion: &headSegmentVersion{
			topLevel: 2,
			levels:   levels,
		},
		writeBlock: blockManager.writeBlock,
		writeSlotIndex: slotIndexManager.writeSlotIndex,
	}, blockManager, slotIndexManager
}

func Test_scan_forward(t *testing.T) {
	should := require.New(t)
	editing, blockManager, slotIndexManager := realEditingHead()
	should.Nil(editing.addBlock(ctx, newBlock(0, []*Entry{
		blobEntry("hello"),
	})))
	should.Nil(editing.addBlock(ctx, newBlock(256, []*Entry{
		blobEntry("dog"),
	})))
	filter := editing.strategy.NewBlobValueFilter(0, "dog")
	iter := editing.headSegmentVersion.scanForward(ctx, blockManager, slotIndexManager, filter)
	chunk, err := iter()
	should.Nil(err)
	rows, err := chunk.search(ctx, nil,0, filter)
	should.Nil(err)
	fmt.Println(chunk)
	should.Equal(1, len(rows))
	should.Equal(1, rows[0].Offset)
}