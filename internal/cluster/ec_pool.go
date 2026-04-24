package cluster

import (
	"sync"

	"github.com/klauspost/reedsolomon"
)

// encoderEntry caches a reedsolomon.Encoder for a given ECConfig.
type encoderEntry struct {
	enc reedsolomon.Encoder
}

// encoderCache maps ECConfig → *encoderEntry.
var encoderCache sync.Map

func getEncoder(cfg ECConfig) (reedsolomon.Encoder, error) {
	if v, ok := encoderCache.Load(cfg); ok {
		return v.(*encoderEntry).enc, nil
	}
	enc, err := reedsolomon.New(cfg.DataShards, cfg.ParityShards)
	if err != nil {
		return nil, err
	}
	entry := &encoderEntry{enc: enc}
	// Store-or-load to handle concurrent first calls for the same config.
	actual, _ := encoderCache.LoadOrStore(cfg, entry)
	return actual.(*encoderEntry).enc, nil
}
