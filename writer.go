package lstore

import (
	"path"
	"fmt"
	"github.com/v2pro/plz/countlog"
	"context"
	"os"
	"github.com/v2pro/plz/concurrent"
	"unsafe"
	"sync/atomic"
	"github.com/esdb/gocodec"
)

func (store *Store) AsyncExecute(ctx context.Context, cmd Command) {
	select {
	case store.commandQueue <- cmd:
	case <-ctx.Done():
	}
}

func (store *Store) startCommandQueue(initialVersion *StoreVersion) {
	store.executor = concurrent.NewUnboundedExecutor()
	store.executor.Go(func(ctx context.Context) {
		currentVersion := initialVersion
		defer func() {
			err := currentVersion.Close()
			if err != nil {
				countlog.Error("event!store.failed to close", "err", err)
			}
		}()
		stream := gocodec.NewStream(nil)
		ctx = context.WithValue(ctx, "stream", stream)
		for {
			var command Command
			select {
			case <-ctx.Done():
				return
			case command = <-store.commandQueue:
			}
			newVersion := handleCommand(ctx, command, currentVersion)
			if newVersion != nil {
				atomic.StorePointer(&store.currentVersion, unsafe.Pointer(newVersion))
				err := currentVersion.Close()
				if err != nil {
					countlog.Error("event!store.failed to close", "err", err)
				}
				currentVersion = newVersion
			}
		}
	})
}

func handleCommand(ctx context.Context, command Command, currentVersion *StoreVersion) *StoreVersion {
	defer func() {
		recovered := recover()
		if recovered != nil && recovered != concurrent.StopSignal {
			countlog.Fatal("event!store.panic",
				"err", recovered,
				"stacktrace", countlog.ProvideStacktrace)
		}
	}()
	return command(ctx, currentVersion)
}

func (version *StoreVersion) AddSegment() (*StoreVersion, error) {
	oldVersion := version
	newVersion := *oldVersion
	newVersion.referenceCounter = 1
	newVersion.rawSegments = make([]*RawSegment, len(oldVersion.rawSegments)+1)
	i := 0
	var err error
	for ; i < len(oldVersion.rawSegments); i++ {
		oldSegment := oldVersion.rawSegments[i]
		newVersion.rawSegments[i], err = openRawSegment(oldSegment.Path)
		if err != nil {
			return nil, err
		}
	}
	conf := oldVersion.config
	rotatedTo := path.Join(conf.Directory, fmt.Sprintf("%d.segment", oldVersion.tailSegment.StartOffset))
	if err = os.Rename(oldVersion.tailSegment.Path, rotatedTo); err != nil {
		return nil, err
	}
	newVersion.rawSegments[i], err = openRawSegment(rotatedTo)
	newVersion.tailSegment, err = openTailSegment(
		conf.TailSegmentPath(), conf.TailSegmentMaxSize, oldVersion.tailSegment.Tail)
	if err != nil {
		return nil, err
	}
	countlog.Info("event!store.rotated", "tail", newVersion.tailSegment.StartOffset)
	return &newVersion, nil
}