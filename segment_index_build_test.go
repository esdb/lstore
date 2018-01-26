package lstore

import (
	"testing"
	"github.com/stretchr/testify/require"
	"github.com/v2pro/plz/countlog"
	"context"
	"github.com/esdb/biter"
	"os"
	"github.com/v2pro/plz/concurrent"
	"github.com/v2pro/plz"
	"fmt"
)

func TestMain(m *testing.M) {
	defer concurrent.GlobalUnboundedExecutor.StopAndWaitForever()
	plz.LogLevel = countlog.LevelTrace
	plz.PlugAndPlay()
	m.Run()
}

func intEntry(values ...int64) *Entry {
	return &Entry{IntValues: values}
}

func blobEntry(values ...Blob) *Entry {
	return &Entry{BlobValues: values}
}

func blobEntries(values ...Blob) []*Entry {
	entries := make([]*Entry, len(values))
	for i, value := range values {
		entries[i] = blobEntry(value)
	}
	return entries
}

func intBlobEntry(intValue int64, blobValue Blob) *Entry {
	return &Entry{IntValues: []int64{intValue}, BlobValues: []Blob{blobValue}}
}

var ctx = countlog.Ctx(context.Background())

type testIndexSegmentObjs struct {
	*indexSegment
	*indexingStrategy
	slotIndexManager slotIndexManager
	slotIndexWriter  slotIndexWriter
	slotIndexReader  slotIndexReader
	blockManager     blockManager
	blockWriter      blockWriter
	fakeBlockWriter  blockWriter
	blockReader      blockReader
}

func (objs *testIndexSegmentObjs) level(i level) *slotIndex {
	slotIndex, err := objs.slotIndexWriter.mapWritableSlotIndex(objs.levels[i], i)
	if err != nil {
		panic(err)
	}
	return slotIndex
}

func (objs *testIndexSegmentObjs) search(i level, filter Filter) biter.Bits {
	return objs.level(i).search(i, filter)
}

func (objs *testIndexSegmentObjs) indexAt(i level, slots ...biter.Slot) *slotIndex {
	slotIndex := objs.level(i)
	for j, slot := range slots {
		seq := slotIndex.children[slot]
		var err error
		slotIndex, err = objs.slotIndexWriter.mapWritableSlotIndex(slotIndexSeq(seq), i-level(j)-1)
		if err != nil {
			panic(err)
		}
	}
	return slotIndex
}

func testIndexSegment() testIndexSegmentObjs {
	strategy := newIndexingStrategy(&indexingStrategyConfig{
		BloomFilterIndexedBlobColumns: []int{0},
	})
	os.RemoveAll("/tmp/store")
	os.Mkdir("/tmp/store", 0777)
	slotIndexManager := newSlotIndexManager(&slotIndexManagerConfig{
		IndexDirectory: "/tmp/store/index",
	}, strategy)
	slotIndexWriter := slotIndexManager.newWriter(14, 4)
	slotIndexReader := slotIndexManager.newReader(14, 4)
	blockManager := newBlockManager(&blockManagerConfig{
		BlockDirectory: "/tmp/store/block",
	})
	blockReader := blockManager.newReader(14, 4)
	blockWriter := blockManager.newWriter()
	indexSegment, err := newIndexSegment(slotIndexWriter, nil)
	if err != nil {
		panic(err)
	}
	return testIndexSegmentObjs{
		indexSegment:     indexSegment,
		indexingStrategy: slotIndexWriter.indexingStrategy(),
		slotIndexManager: slotIndexManager,
		slotIndexReader:  slotIndexReader,
		slotIndexWriter:  slotIndexWriter,
		blockManager:     blockManager,
		blockReader:      blockReader,
		blockWriter:      blockWriter,
		fakeBlockWriter:  &fakeBlockWriter{},
	}
}

type fakeBlockWriter struct {
}

func (writer *fakeBlockWriter) writeBlock(seq blockSeq, block *block) (blockSeq, blockSeq, error) {
	return seq, seq + 6, nil
}

func (writer *fakeBlockWriter) remove(untilSeq blockSeq) {
}

func (writer *fakeBlockWriter) Close() error {
	return nil
}

func Test_add_first_block(t *testing.T) {
	should := require.New(t)
	segment := testIndexSegment()
	segment.addBlock(ctx, segment.slotIndexWriter, segment.fakeBlockWriter,
		newBlock(0, []*Entry{
			blobEntry("hello"),
		}))
	should.Equal(blockSeq(1+6), segment.tailBlockSeq)
	should.Equal([]uint64{1}, segment.level(0).children[:1])
	filterHello := segment.NewBlobValueFilter(0, "hello")
	result := segment.search(0, filterHello)
	should.Equal(biter.SetBits[0], result)
	result = segment.search(1, filterHello)
	should.Equal(biter.SetBits[0], result)
	result = segment.search(2, filterHello)
	should.Equal(biter.SetBits[0], result)
	filter123 := segment.NewBlobValueFilter(0, "123")
	result = segment.search(2, filter123)
	should.Equal(biter.Bits(0), result)
}

func Test_add_two_blocks(t *testing.T) {
	should := require.New(t)
	segment := testIndexSegment()
	should.Nil(segment.addBlock(ctx, segment.slotIndexWriter, segment.fakeBlockWriter,
		newBlock(0, []*Entry{
			blobEntry("hello"),
		})))
	segment.addBlock(ctx, segment.slotIndexWriter, segment.fakeBlockWriter,
		newBlock(0, []*Entry{
			blobEntry("world"),
		}))
	should.Equal(blockSeq(1+12), segment.tailBlockSeq)
	should.Equal([]uint64{1, 7}, segment.level(0).children[:2])
	filterHello := segment.NewBlobValueFilter(0, "hello")
	result := segment.search(0, filterHello)
	should.Equal(biter.SetBits[0], result)
	filterWorld := segment.NewBlobValueFilter(0, "world")
	result = segment.search(0, filterWorld)
	should.Equal(biter.SetBits[1], result)
}

func Test_add_64_blocks(t *testing.T) {
	should := require.New(t)
	segment := testIndexSegment()
	for i := 0; i < 64; i++ {
		segment.addBlock(ctx, segment.slotIndexWriter, segment.fakeBlockWriter,
			newBlock(0, []*Entry{
				blobEntry(Blob(fmt.Sprintf("hello%d", i))),
			}))
	}
	should.Equal(biter.Slot(0), *segment.level(0).tailSlot)
	should.Equal(biter.Slot(2), *segment.level(1).tailSlot)
	should.Equal(blockSeq(1+6*64), segment.tailBlockSeq)
	result := segment.indexAt(1, 0).search(0, segment.NewBlobValueFilter(0, "hello0"))
	should.Equal(biter.SetBits[0], result&biter.SetBits[0])
	result = segment.indexAt(1, 0).search(0, segment.NewBlobValueFilter(0, "hello63"))
	should.Equal(biter.SetBits[63], result&biter.SetBits[63])
}

func Test_add_65_blocks(t *testing.T) {
	should := require.New(t)
	segment := testIndexSegment()
	for i := 0; i < 65; i++ {
		segment.addBlock(ctx, segment.slotIndexWriter, segment.fakeBlockWriter,
			newBlock(0, []*Entry{
				blobEntry(Blob(fmt.Sprintf("hello%d", i))),
			}))
	}
	should.Equal(biter.Slot(1), *segment.level(0).tailSlot)
	should.Equal(biter.Slot(2), *segment.level(1).tailSlot)
	should.Equal(blockSeq(1+6*65), segment.tailBlockSeq)
	should.Equal([]uint64{1}, segment.level(1).children[:1])
	result := segment.search(0, segment.NewBlobValueFilter(0, "hello0"))
	should.Equal(biter.Bits(0), result, "level0 moved on, forget the old values")
	result = segment.search(0, segment.NewBlobValueFilter(0, "hello64"))
	should.Equal(biter.SetBits[0], result)
	result = segment.search(1, segment.NewBlobValueFilter(0, "hello0"))
	should.Equal(biter.SetBits[0], result, "level1 still remembers")
	result = segment.search(1, segment.NewBlobValueFilter(0, "hello64"))
	should.Equal(biter.SetBits[1], result)
}

func Test_add_66_blocks(t *testing.T) {
	should := require.New(t)
	segment := testIndexSegment()
	for i := 0; i < 66; i++ {
		segment.addBlock(ctx, segment.slotIndexWriter, segment.fakeBlockWriter,
			newBlock(0, []*Entry{
				blobEntry(Blob(fmt.Sprintf("hello%d", i))),
			}))
	}
	should.Equal(biter.Slot(2), *segment.level(0).tailSlot)
	should.Equal(biter.Slot(2), *segment.level(1).tailSlot)
	should.Equal(blockSeq(1+6*66), segment.tailBlockSeq)
	should.Equal([]uint64{1}, segment.level(1).children[:1])
	result := segment.search(0, segment.NewBlobValueFilter(0, "hello65"))
	should.Equal(biter.SetBits[1], result)
	result = segment.search(1, segment.NewBlobValueFilter(0, "hello65"))
	should.Equal(biter.SetBits[1], result)
}

func Test_add_129_blocks(t *testing.T) {
	should := require.New(t)
	segment := testIndexSegment()
	for i := 0; i < 129; i++ {
		segment.addBlock(ctx, segment.slotIndexWriter, segment.fakeBlockWriter,
			newBlock(0, []*Entry{
				blobEntry(Blob(fmt.Sprintf("hello%d", i))),
			}))
	}
	should.Equal(biter.Slot(1), *segment.level(0).tailSlot)
	should.Equal(biter.Slot(3), *segment.level(1).tailSlot)
	should.Equal(blockSeq(1+6*129), segment.tailBlockSeq)
	result := segment.search(0, segment.NewBlobValueFilter(0, "hello128"))
	should.Equal(biter.SetBits[0], result)
	result = segment.search(1, segment.NewBlobValueFilter(0, "hello63"))
	should.Equal(biter.SetBits[0], result)
	result = segment.search(1, segment.NewBlobValueFilter(0, "hello64"))
	should.Equal(biter.SetBits[1], result)
	result = segment.search(1, segment.NewBlobValueFilter(0, "hello127"))
	should.Equal(biter.SetBits[1], result)
	result = segment.search(1, segment.NewBlobValueFilter(0, "hello128"))
	should.Equal(biter.SetBits[2], result)
}

func Test_add_64x64_blocks(t *testing.T) {
	blockLength = 2
	should := require.New(t)
	segment := testIndexSegment()
	for i := 0; i < 4096; i++ {
		segment.addBlock(ctx, segment.slotIndexWriter, segment.fakeBlockWriter,
			newBlock(0, []*Entry{
				blobEntry(Blob(fmt.Sprintf("hello%d", i))),
			}))
	}
	should.Equal(biter.Slot(0), *segment.level(0).tailSlot)
	should.Equal(biter.Slot(1), *segment.level(1).tailSlot)
	should.Equal(biter.Slot(2), *segment.level(2).tailSlot)
	should.Equal(blockSeq(1+6*4096), segment.tailBlockSeq)
	result := segment.indexAt(2, 0).search(1, segment.NewBlobValueFilter(0, "hello4095"))
	should.Equal(biter.SetBits[63], result)
	result = segment.indexAt(2, 0, 63).search(0, segment.NewBlobValueFilter(0, "hello4095"))
	should.Equal(biter.SetBits[63], result)
}

func Test_add_64x64_plus_1_blocks(t *testing.T) {
	blockLength = 2
	should := require.New(t)
	segment := testIndexSegment()
	for i := 0; i < 4097; i++ {
		segment.addBlock(ctx, segment.slotIndexWriter, segment.fakeBlockWriter,
			newBlock(0, []*Entry{
				blobEntry(Blob(fmt.Sprintf("hello%d", i))),
			}))
	}
	should.Equal(blockSeq(1+6*4097), segment.tailBlockSeq)
	result := segment.search(0, segment.NewBlobValueFilter(0, "hello4096"))
	should.Equal(biter.SetBits[0], result)
	result = segment.search(0, segment.NewBlobValueFilter(0, "hello4095"))
	should.Equal(biter.Bits(0), result, "level0 forget")
	result = segment.search(1, segment.NewBlobValueFilter(0, "hello4096"))
	should.Equal(biter.SetBits[0], result)
	result = segment.search(1, segment.NewBlobValueFilter(0, "hello4095"))
	should.Equal(biter.Bits(0), result, "level1 forget")
	result = segment.search(2, segment.NewBlobValueFilter(0, "hello0"))
	should.Equal(biter.SetBits[0], result, "level2 still remembers")
	result = segment.search(2, segment.NewBlobValueFilter(0, "hello4095"))
	should.Equal(biter.SetBits[0], result, "level2 still remembers")
	result = segment.search(2, segment.NewBlobValueFilter(0, "hello4096"))
	should.Equal(biter.SetBits[1], result, "level2 still remembers")
}

func Test_add_64x64x64_blocks(t *testing.T) {
	blockLength = 2
	should := require.New(t)
	segment := testIndexSegment()
	for i := 0; i < 64*64*64; i++ {
		segment.addBlock(ctx, segment.slotIndexWriter, segment.fakeBlockWriter, newBlock(0, []*Entry{
			blobEntry(Blob(fmt.Sprintf("hello%d", i))),
		}))
	}
	should.Equal(biter.Slot(0), *segment.level(0).tailSlot)
	should.Equal(biter.Slot(1), *segment.level(1).tailSlot)
	should.Equal(biter.Slot(1), *segment.level(2).tailSlot)
	should.Equal(biter.Slot(2), *segment.level(3).tailSlot)
	should.Equal(blockSeq(1+6*64*64*64), segment.tailBlockSeq)
	result := segment.indexAt(3, 0, 63, 63).search(0, segment.NewBlobValueFilter(0, "hello262143"))
	should.Equal(biter.SetBits[63], result)
	result = segment.indexAt(3, 0, 63).search(1, segment.NewBlobValueFilter(0, "hello262143"))
	should.Equal(biter.SetBits[63], result)
	result = segment.indexAt(3, 0).search(2, segment.NewBlobValueFilter(0, "hello262143"))
	should.Equal(biter.SetBits[63], result)
}

func Test_add_64x64x64_plus_1_blocks(t *testing.T) {
	blockLength = 2
	should := require.New(t)
	segment := testIndexSegment()
	for i := 0; i < 64*64*64+1; i++ {
		segment.addBlock(ctx, segment.slotIndexWriter, segment.fakeBlockWriter,
			newBlock(0, []*Entry{
				blobEntry(Blob(fmt.Sprintf("hello%d", i))),
			}))
	}
	should.Equal(biter.Slot(1), *segment.level(0).tailSlot)
	should.Equal(biter.Slot(1), *segment.level(1).tailSlot)
	should.Equal(biter.Slot(1), *segment.level(2).tailSlot)
	should.Equal(biter.Slot(2), *segment.level(3).tailSlot)
	should.Equal(blockSeq(1+6*(64*64*64+1)), segment.tailBlockSeq)
	result := segment.search(0, segment.NewBlobValueFilter(0, "hello262144"))
	should.Equal(biter.SetBits[0], result)
	result = segment.search(1, segment.NewBlobValueFilter(0, "hello262144"))
	should.Equal(biter.SetBits[0], result)
	result = segment.search(2, segment.NewBlobValueFilter(0, "hello262144"))
	should.Equal(biter.SetBits[0], result)
	should.Equal(level(3), segment.topLevel)
	result = segment.search(3, segment.NewBlobValueFilter(0, "hello262143"))
	should.Equal(biter.SetBits[0], result)
	result = segment.search(3, segment.NewBlobValueFilter(0, "hello262144"))
	should.Equal(biter.SetBits[1], result)
}

func Test_add_64x64x64x2_plus_1_blocks(t *testing.T) {
	blockLength = 2
	should := require.New(t)
	segment := testIndexSegment()
	for i := 0; i < 64*64*64*2+1; i++ {
		segment.addBlock(ctx, segment.slotIndexWriter, segment.fakeBlockWriter,
			newBlock(0, []*Entry{
				blobEntry(Blob(fmt.Sprintf("hello%d", i))),
			}))
	}
	should.Equal(blockSeq(1+6*(64*64*64*2+1)), segment.tailBlockSeq)
	result := segment.search(0, segment.NewBlobValueFilter(0, "hello524288"))
	should.Equal(biter.SetBits[0], result)
	result = segment.search(1, segment.NewBlobValueFilter(0, "hello524288"))
	should.Equal(biter.SetBits[0], result)
	result = segment.search(2, segment.NewBlobValueFilter(0, "hello524288"))
	should.Equal(biter.SetBits[0], result)
	should.Equal(level(3), segment.topLevel)
	result = segment.search(3, segment.NewBlobValueFilter(0, "hello262143"))
	should.Equal(biter.SetBits[0], result)
	result = segment.search(3, segment.NewBlobValueFilter(0, "hello262144"))
	should.Equal(biter.SetBits[1], result)
	result = segment.search(3, segment.NewBlobValueFilter(0, "hello524287"))
	should.Equal(biter.SetBits[1], result)
	result = segment.search(3, segment.NewBlobValueFilter(0, "hello524288"))
	should.Equal(biter.SetBits[2], result)
}
//
//func Test_add_64x64x64x64_plus_1_blocks(t *testing.T) {
//	blockLength = 2
//	blockLengthInPowerOfTwo = 1
//	should := require.New(t)
//	segment := testIndexSegment()
//	segment.tailOffset = Offset(64 * 64 * 64 * 64 * blockLength)
//	segment.addBlock(ctx, segment.slotIndexWriter, segment.fakeBlockWriter,
//		newBlock(0, []*Entry{
//			blobEntry("final block"),
//		}))
//	should.Equal(level(4), segment.topLevel)
//	result := segment.search(0, segment.NewBlobValueFilter(0, "final block"))
//	should.Equal(biter.SetBits[0], result)
//	result = segment.search(1, segment.NewBlobValueFilter(0, "final block"))
//	should.Equal(biter.SetBits[0], result)
//	result = segment.search(2, segment.NewBlobValueFilter(0, "final block"))
//	should.Equal(biter.SetBits[0], result)
//	result = segment.search(3, segment.NewBlobValueFilter(0, "final block"))
//	should.Equal(biter.SetBits[0], result)
//	result = segment.search(4, segment.NewBlobValueFilter(0, "final block"))
//	should.Equal(biter.SetBits[1], result)
//}
//
//func Test_add_64x64x64x64x64_plus_1_blocks(t *testing.T) {
//	blockLength = 2
//	blockLengthInPowerOfTwo = 1
//	should := require.New(t)
//	segment := testIndexSegment()
//	segment.tailOffset = Offset(64 * 64 * 64 * 64 * 64 * blockLength)
//	segment.addBlock(ctx, segment.slotIndexWriter, segment.fakeBlockWriter,
//		newBlock(0, []*Entry{
//			blobEntry("final block"),
//		}))
//	should.Equal(level(5), segment.topLevel)
//	result := segment.search(0, segment.NewBlobValueFilter(0, "final block"))
//	should.Equal(biter.SetBits[0], result)
//	result = segment.search(1, segment.NewBlobValueFilter(0, "final block"))
//	should.Equal(biter.SetBits[0], result)
//	result = segment.search(2, segment.NewBlobValueFilter(0, "final block"))
//	should.Equal(biter.SetBits[0], result)
//	result = segment.search(3, segment.NewBlobValueFilter(0, "final block"))
//	should.Equal(biter.SetBits[0], result)
//	result = segment.search(4, segment.NewBlobValueFilter(0, "final block"))
//	should.Equal(biter.SetBits[0], result)
//	result = segment.search(5, segment.NewBlobValueFilter(0, "final block"))
//	should.Equal(biter.SetBits[1], result)
//}
//
//func Test_add_64x64x64x64x64x64_plus_1_blocks(t *testing.T) {
//	blockLength = 2
//	blockLengthInPowerOfTwo = 1
//	should := require.New(t)
//	segment := testIndexSegment()
//	segment.tailOffset = Offset(64 * 64 * 64 * 64 * 64 * 64 * blockLength)
//	segment.addBlock(ctx, segment.slotIndexWriter, segment.fakeBlockWriter,
//		newBlock(0, []*Entry{
//			blobEntry("final block"),
//		}))
//	should.Equal(level(6), segment.topLevel)
//	result := segment.search(0, segment.NewBlobValueFilter(0, "final block"))
//	should.Equal(biter.SetBits[0], result)
//	result = segment.search(1, segment.NewBlobValueFilter(0, "final block"))
//	should.Equal(biter.SetBits[0], result)
//	result = segment.search(2, segment.NewBlobValueFilter(0, "final block"))
//	should.Equal(biter.SetBits[0], result)
//	result = segment.search(3, segment.NewBlobValueFilter(0, "final block"))
//	should.Equal(biter.SetBits[0], result)
//	result = segment.search(4, segment.NewBlobValueFilter(0, "final block"))
//	should.Equal(biter.SetBits[0], result)
//	result = segment.search(5, segment.NewBlobValueFilter(0, "final block"))
//	should.Equal(biter.SetBits[0], result)
//	result = segment.search(6, segment.NewBlobValueFilter(0, "final block"))
//	should.Equal(biter.SetBits[1], result)
//}
//
//func Test_add_64x64x64x64x64x64x64_plus_1_blocks(t *testing.T) {
//	blockLength = 2
//	blockLengthInPowerOfTwo = 1
//	should := require.New(t)
//	segment := testIndexSegment()
//	segment.tailOffset = Offset(64 * 64 * 64 * 64 * 64 * 64 * 64 * blockLength)
//	segment.addBlock(ctx, segment.slotIndexWriter, segment.fakeBlockWriter,
//		newBlock(0, []*Entry{
//			blobEntry("final block"),
//		}))
//	segment.addBlock(ctx, segment.slotIndexWriter, segment.fakeBlockWriter,
//		newBlock(0, []*Entry{
//			blobEntry("final final block"),
//		}))
//	should.Equal(level(7), segment.topLevel)
//	result := segment.search(0, segment.NewBlobValueFilter(0, "final block"))
//	should.Equal(biter.SetBits[0], result)
//	result = segment.search(0, segment.NewBlobValueFilter(0, "final final block"))
//	should.Equal(biter.SetBits[1], result)
//	result = segment.search(1, segment.NewBlobValueFilter(0, "final block"))
//	should.Equal(biter.SetBits[0], result)
//	result = segment.search(1, segment.NewBlobValueFilter(0, "final final block"))
//	should.Equal(biter.SetBits[0], result)
//	result = segment.search(2, segment.NewBlobValueFilter(0, "final block"))
//	should.Equal(biter.SetBits[0], result)
//	result = segment.search(3, segment.NewBlobValueFilter(0, "final block"))
//	should.Equal(biter.SetBits[0], result)
//	result = segment.search(4, segment.NewBlobValueFilter(0, "final block"))
//	should.Equal(biter.SetBits[0], result)
//	result = segment.search(5, segment.NewBlobValueFilter(0, "final block"))
//	should.Equal(biter.SetBits[0], result)
//	result = segment.search(6, segment.NewBlobValueFilter(0, "final block"))
//	should.Equal(biter.SetBits[0], result)
//	result = segment.search(7, segment.NewBlobValueFilter(0, "final block"))
//	should.Equal(biter.SetBits[1], result)
//	result = segment.search(7, segment.NewBlobValueFilter(0, "final final block"))
//	should.Equal(biter.SetBits[1], result)
//}
//
//func Test_add_64x64x64x64x64x64x64x64_plus_1_blocks(t *testing.T) {
//	blockLength = 2
//	blockLengthInPowerOfTwo = 1
//	should := require.New(t)
//	segment := testIndexSegment()
//	segment.tailOffset = Offset(64 * 64 * 64 * 64 * 64 * 64 * 64 * 64 * blockLength)
//	segment.addBlock(ctx, segment.slotIndexWriter, segment.fakeBlockWriter,
//		newBlock(0, []*Entry{
//			blobEntry("final block"),
//		}))
//	should.Equal(level(8), segment.topLevel)
//	result := segment.search(0, segment.NewBlobValueFilter(0, "final block"))
//	should.Equal(biter.SetBits[0], result)
//	result = segment.search(1, segment.NewBlobValueFilter(0, "final block"))
//	should.Equal(biter.SetBits[0], result)
//	result = segment.search(2, segment.NewBlobValueFilter(0, "final block"))
//	should.Equal(biter.SetBits[0], result)
//	result = segment.search(3, segment.NewBlobValueFilter(0, "final block"))
//	should.Equal(biter.SetBits[0], result)
//	result = segment.search(4, segment.NewBlobValueFilter(0, "final block"))
//	should.Equal(biter.SetBits[0], result)
//	result = segment.search(5, segment.NewBlobValueFilter(0, "final block"))
//	should.Equal(biter.SetBits[0], result)
//	result = segment.search(6, segment.NewBlobValueFilter(0, "final block"))
//	should.Equal(biter.SetBits[0], result)
//	result = segment.search(7, segment.NewBlobValueFilter(0, "final block"))
//	should.Equal(biter.SetBits[0], result)
//	result = segment.search(8, segment.NewBlobValueFilter(0, "final block"))
//	should.Equal(biter.SetBits[1], result)
//}
