package lstore

import (
	"github.com/esdb/gocodec"
	"github.com/v2pro/plz/countlog"
	"io/ioutil"
	"os"
)

func (remover *remover) loadTombstone(ctx *countlog.Context) error {
	content, err := ioutil.ReadFile(remover.cfg.TombstoneSegmentPath())
	if os.IsNotExist(err) {
		return nil
	}
	tombstoneObj, err := gocodec.Unmarshal(content, (*segmentHeader)(nil))
	if err != nil {
		return err
	}
	return remover.doRemove(ctx, tombstoneObj.(*segmentHeader).headOffset)
}
