package lstore

import (
	"github.com/v2pro/plz/countlog"
	"time"
	"errors"
	"context"
	"os"
	"github.com/edsrzf/mmap-go"
	"github.com/esdb/gocodec"
	"io"
	"github.com/v2pro/plz"
)

type indexerCommand func(ctx countlog.Context)

type indexer struct {
	store          *Store
	commandQueue   chan indexerCommand
	currentVersion *StoreVersion
	head           *headSegmentVersion
	headFile       *os.File
	headMMap       mmap.MMap
}

func (store *Store) newIndexer(ctx countlog.Context) (*indexer, error) {
	indexer := &indexer{
		store:          store,
		currentVersion: store.latest(),
		commandQueue:   make(chan indexerCommand, 1),
	}
	if err := indexer.loadHead(ctx); err != nil {
		return nil, err
	}
	indexer.start()
	return indexer, nil
}

func (indexer *indexer) loadHead(ctx countlog.Context) error {
	file, err := os.OpenFile(indexer.store.HeadSegmentPath(), os.O_RDWR, 0666)
	ctx.TraceCall("callee!os.OpenFile", err)
	if err != nil {
		return err
	}
	indexer.headFile = file
	headMMap, err := mmap.Map(file, mmap.RDWR, 0)
	ctx.TraceCall("callee!mmap.Map", err)
	if err != nil {
		return err
	}
	indexer.headMMap = headMMap
	iter := gocodec.NewIterator(headMMap)
	head, _ := iter.Unmarshal((*headSegmentVersion)(nil)).(*headSegmentVersion)
	ctx.TraceCall("callee!iter.Unmarshal", iter.Error)
	if iter.Error != nil {
		return iter.Error
	}
	indexer.head = head
	return nil
}

func (indexer *indexer) Close() error {
	return plz.CloseAll([]io.Closer{
		indexer.headFile,
		plz.WrapCloser(indexer.headMMap.Unmap),
	})
}

func (indexer *indexer) start() {
	store := indexer.store
	indexer.currentVersion = store.latest()
	store.executor.Go(func(ctxObj context.Context) {
		ctx := countlog.Ctx(ctxObj)
		defer func() {
			countlog.Info("event!indexer.stop")
			err := indexer.currentVersion.Close()
			if err != nil {
				countlog.Error("event!store.failed to close", "err", err)
			}
		}()
		countlog.Info("event!indexer.start")
		for {
			timer := time.NewTimer(time.Second * 10)
			var cmd indexerCommand
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			case cmd = <-indexer.commandQueue:
			}
			if store.isLatest(indexer.currentVersion) {
				if cmd == nil {
					return
				}
			} else {
				if err := indexer.currentVersion.Close(); err != nil {
					countlog.Error("event!indexer.failed to close version", "err", err)
				}
				indexer.currentVersion = store.latest()
			}
			if cmd == nil {
				cmd = func(ctx countlog.Context) {
					indexer.doIndex(ctx)
				}
			}
			cmd(ctx)
		}
	})
}

func (indexer *indexer) asyncExecute(cmd indexerCommand) error {
	select {
	case indexer.commandQueue <- cmd:
		return nil
	default:
		return errors.New("too many compaction request")
	}
}

func (indexer *indexer) Index() error {
	resultChan := make(chan error)
	indexer.asyncExecute(func(ctx countlog.Context) {
		resultChan <- indexer.doIndex(ctx)
	})
	return <-resultChan
}

func (indexer *indexer) doIndex(ctx countlog.Context) (err error) {
	//// version will not change during compaction
	//store := indexer.currentVersion
	//blockManager := indexer.store.blockManager
	//strategy := indexer.store.indexingStrategy
	//countlog.Trace("event!indexer.run")
	//if len(store.rawSegments) == 0 {
	//	return nil
	//}
	//tailBlockSeq := indexer.head.tailBlockSeq
	//tailOffset := indexer.head.tailOffset
	//purgedRawSegmentsCount := 0
	//for _, rawSegment := range store.rawSegments {
	//	purgedRawSegmentsCount++
	//	// TODO: ensure rawSegment is actually blockLength
	//}
	//// ensure blocks are persisted
	//indexer.head.tailBlockSeq = tailBlockSeq
	//err = indexer.headMMap.Flush()
	//countlog.TraceCall("callee!headMMap.Flush", err, "tailBlockSeq", indexer.head.tailBlockSeq)
	//err = indexer.saveIndices(ctx, level0SlotIndex, level1SlotIndex, level2SlotIndex)
	//countlog.TraceCall("callee!indexer.saveIndices", err)
	//err = indexer.store.writer.switchIndexedSegment(ctx, store.headSegment, purgedRawSegmentsCount)
	//if err != nil {
	//	return err
	//}
	return nil
}