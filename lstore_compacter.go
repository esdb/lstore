package lstore

import (
	"context"
	"github.com/v2pro/plz/countlog"
	"errors"
)

type compacterCommand func(ctx context.Context)

type compacter struct {
	store          *Store
	commandQueue   chan compacterCommand
}

func (store *Store) newCompacter() *compacter {
	compacter := &compacter{
		store:          store,
		commandQueue:   make(chan compacterCommand, 1),
	}
	compacter.start()
	return compacter
}

func (compacter *compacter) start() {
	store := compacter.store
	store.executor.Go(func(ctx context.Context) {
		defer func() {
			countlog.Info("event!compacter.stop")
		}()
		countlog.Info("event!compacter.start")
		for {
			var cmd compacterCommand
			select {
			case <-ctx.Done():
				return
			case cmd = <-compacter.commandQueue:
			}
			cmd(ctx)
		}
	})
}

func (compacter *compacter) asyncExecute(cmd compacterCommand) error {
	select {
	case compacter.commandQueue <- cmd:
		return nil
	default:
		return errors.New("too many compaction request")
	}
}

// purgeIndexedSegments should only be called from indexer
func (compacter *compacter) purgeIndexedSegments() error {
	resultChan := make(chan error)
	compacter.asyncExecute(func(ctx context.Context) {

	})
	return <-resultChan
}