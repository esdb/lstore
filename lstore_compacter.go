package lstore

import (
	"context"
	"github.com/v2pro/plz/countlog"
	"time"
)

type compacter struct {
	store *Store
	currentVersion *StoreVersion
}

func (store *Store) newCompacter() *compacter {
	compacter := &compacter{store: store, currentVersion: store.latest()}
	compacter.start()
	return compacter
}

func (compacter *compacter) start() {
	store := compacter.store
	compacter.currentVersion = store.latest()
	store.executor.Go(func(ctx context.Context) {
		defer func() {
			err := compacter.currentVersion.Close()
			if err != nil {
				countlog.Error("event!store.failed to close", "err", err)
			}
		}()
		if store.CompactAfterStartup {
			compacter.compact()
		}
		for {
			timer := time.NewTimer(time.Second * 10)
			select {
			case<-ctx.Done():
				return
			case <-timer.C:
			}
			if store.isLatest(compacter.currentVersion) {
				// nothing to compact
				continue
			}
			if err := compacter.currentVersion.Close(); err != nil {
				countlog.Error("event!compacter.failed to close version", "err", err)
			}
			compacter.currentVersion = store.latest()
			compacter.compact()
		}
	})
}

func (compacter *compacter) compact() {
	countlog.Trace("event!compacter.run")
}
