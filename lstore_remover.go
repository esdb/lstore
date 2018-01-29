package lstore

import (
	"context"
	"github.com/esdb/gocodec"
	"github.com/v2pro/plz/concurrent"
	"github.com/v2pro/plz/countlog"
	"io/ioutil"
	"os"
)

type removerCommand func(ctx *countlog.Context)

type remover struct {
	cfg          *removerConfig
	indexer      *indexer
	state        *storeState
	commandQueue chan removerCommand
	stream       *gocodec.Stream
}

func (store *Store) newRemover(ctx *countlog.Context) (*remover, error) {
	cfg := store.cfg
	remover := &remover{
		cfg:          &cfg.removerConfig,
		indexer:      store.indexer,
		state:        &store.storeState,
		commandQueue: make(chan removerCommand),
		stream:       gocodec.NewStream(nil),
	}
	err := remover.loadTombstone(ctx)
	if err != nil {
		return nil, err
	}
	remover.start(store.executor)
	return remover, nil
}

func (remover *remover) start(executor *concurrent.UnboundedExecutor) {
	executor.Go(func(ctx *countlog.Context) {
		defer func() {
			countlog.Info("event!indexer.stop")
		}()
		countlog.Info("event!indexer.start")
		for {
			var cmd removerCommand
			select {
			case <-ctx.Done():
				return
			case cmd = <-remover.commandQueue:
			}
			remover.runCommand(ctx, cmd)
		}
	})
}

func (remover *remover) runCommand(ctx *countlog.Context, cmd removerCommand) {
	defer func() {
		recovered := recover()
		if recovered == concurrent.StopSignal {
			panic(concurrent.StopSignal)
		}
		countlog.LogPanic(recovered)
	}()
	cmd(ctx)
}

func (remover *remover) asyncExecute(ctx *countlog.Context, cmd removerCommand) error {
	select {
	case remover.commandQueue <- cmd:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (remover *remover) Remove(ctxObj context.Context, removingFrom Offset) error {
	ctx := countlog.Ctx(ctxObj)
	resultChan := make(chan error)
	remover.asyncExecute(ctx, func(ctx *countlog.Context) {
		resultChan <- remover.doRemove(ctx, removingFrom)
	})
	return <-resultChan
}

func (remover *remover) doRemove(ctx *countlog.Context, removingFrom Offset) error {
	err := remover.writeTombstone(ctx, removingFrom)
	if err != nil {
		return err
	}
	removedSegments, removedFrom := remover.state.removeHead(removingFrom)
	remover.indexer.removeIndexedSegments(ctx, removedSegments)
	if removedFrom == removingFrom {
		err = os.Remove(remover.cfg.TombstoneSegmentPath())
		ctx.TraceCall("callee!os.Remove", err)
	}
	return nil
}

func (remover *remover) writeTombstone(ctx *countlog.Context, removingOffset Offset) error {
	stream := remover.stream
	stream.Reset(nil)
	stream.Marshal(segmentHeader{
		segmentType: segmentTypeTombstone,
		headOffset:  removingOffset,
	})
	if stream.Error != nil {
		return stream.Error
	}
	err := ioutil.WriteFile(remover.cfg.TombstoneSegmentTmpPath(), stream.Buffer(), 0666)
	ctx.TraceCall("callee!ioutil.WriteFile", err)
	if err != nil {
		return err
	}
	err = os.Rename(remover.cfg.TombstoneSegmentTmpPath(), remover.cfg.TombstoneSegmentPath())
	ctx.TraceCall("callee!os.Rename", err)
	if err != nil {
		return err
	}
	return nil
}
