package lstore

import (
	"context"
	"github.com/v2pro/plz/countlog"
	"time"
	"errors"
)

type compactionRequest chan error

func (req compactionRequest) Completed(err error) {
	req <- err
}

type compacter struct {
	store                 *Store
	compactionRequestChan chan compactionRequest
	currentVersion        *StoreVersion
}

func (store *Store) newCompacter() *compacter {
	compacter := &compacter{
		store:                 store,
		currentVersion:        store.latest(),
		compactionRequestChan: make(chan compactionRequest, 1),
	}
	compacter.start()
	return compacter
}

func (compacter *compacter) Compact() error {
	request := make(compactionRequest)
	select {
	case compacter.compactionRequestChan <- request:
	default:
		return errors.New("too many compaction request")
	}
	return <-request
}

func (compacter *compacter) start() {
	store := compacter.store
	compacter.currentVersion = store.latest()
	store.executor.Go(func(ctx context.Context) {
		defer func() {
			countlog.Info("event!compacter.stop")
			err := compacter.currentVersion.Close()
			if err != nil {
				countlog.Error("event!store.failed to close", "err", err)
			}
		}()
		countlog.Info("event!compacter.start")
		if store.CompactAfterStartup {
			compacter.compact(nil)
		}
		for {
			timer := time.NewTimer(time.Second * 10)
			var compactionReq compactionRequest
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			case compactionReq = <-compacter.compactionRequestChan:
			}
			if store.isLatest(compacter.currentVersion) {
				// nothing to compact
				continue
			}
			if err := compacter.currentVersion.Close(); err != nil {
				countlog.Error("event!compacter.failed to close version", "err", err)
			}
			compacter.currentVersion = store.latest()
			compacter.compact(compactionReq)
		}
	})
}

func (compacter *compacter) compact(compactionReq compactionRequest) {
	// version will not change during compaction
	store := compacter.currentVersion
	countlog.Trace("event!compacter.run")
	if len(store.rawSegments) == 0 {
		return
	}
	compactionReq.Completed(nil)
}
