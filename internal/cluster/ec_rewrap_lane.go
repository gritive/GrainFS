package cluster

import (
	"context"
	"strconv"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/scrubber"
)

// ecRewrapBackend is the EC rewrap lane's use-site view of DistributedBackend.
// *DistributedBackend satisfies it structurally (see the assertion below).
type ecRewrapBackend interface {
	NodeID() string
	ListScrubBuckets() ([]string, error)
	ScanObjects(bucket string) (<-chan scrubber.ObjectRecord, error)
	OwnedShards(bucket, key, versionID, nodeID string) []int
	ObjectExists(bucket, key string) (bool, error)
	RewrapShardIfStale(bucket, key, versionID string, shardIdx int, activeGen uint32) (bool, error)
}

var _ ecRewrapBackend = (*DistributedBackend)(nil)

// ECRewrapLane is the encrypt.RewrapLane for EC shards. On a DEK rotation it
// sweeps every owned shard of every live object and migrates any whose on-disk
// gen differs from the active gen. The sweep IGNORES the rotation's oldGen, so
// it is idempotent and tolerant of multiple un-migrated generations.
type ECRewrapLane struct {
	be ecRewrapBackend
}

// ECRewrapLane satisfies encrypt.RewrapLane (it is not registered here; a later
// task wires it into the controller).
var _ encrypt.RewrapLane = (*ECRewrapLane)(nil)

// NewECRewrapLane constructs the EC rewrap lane over be.
func NewECRewrapLane(be ecRewrapBackend) *ECRewrapLane { return &ECRewrapLane{be: be} }

// Name identifies the lane in rewrap orchestration.
func (l *ECRewrapLane) Name() string { return "ec" }

// RewrapByGen sweeps owned EC shards onto activeGen. oldGen is intentionally
// ignored (sweep semantics: migrate any shard whose gen != activeGen).
func (l *ECRewrapLane) RewrapByGen(ctx context.Context, _ uint32, activeGen uint32) error {
	buckets, err := l.be.ListScrubBuckets()
	if err != nil {
		return err
	}
	for _, bucket := range buckets {
		ch, err := l.be.ScanObjects(bucket)
		if err != nil {
			return err
		}
		for rec := range ch {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// The latest version may have been deleted between the scan and now.
			if ok, _ := l.be.ObjectExists(bucket, rec.Key); !ok {
				continue
			}
			for _, idx := range l.be.OwnedShards(bucket, rec.Key, rec.VersionID, l.be.NodeID()) {
				did, err := l.be.RewrapShardIfStale(bucket, rec.Key, rec.VersionID, idx, activeGen)
				if err != nil {
					return err
				}
				if did {
					RewrapECShardsTotal.WithLabelValues(strconv.FormatUint(uint64(activeGen), 10)).Inc()
				}
			}
		}
	}
	return nil
}
