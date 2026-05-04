package scrubber

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"

	"github.com/gritive/GrainFS/internal/storage"
)

// ObjectWalker is the slim subset of storage.Backend the replication scrub
// source needs. Defined locally so this package does not pull the entire
// storage interface.
type ObjectWalker interface {
	WalkObjects(ctx context.Context, bucket, prefix string, fn func(*storage.Object) error) error
}

// ReplicationObjectSource is a generic BlockSource for non-EC (replicated)
// objects. It walks a bucket+prefix domain and emits one Block per stored
// object. Use one instance per (bucket, prefix) you want to scrub — for
// example, the volume layer wires it with bucket=__grainfs_volumes and
// prefix=__vol/, and future internal buckets wire their own instance.
//
// ScopeFull and ScopeLive walk identically here. Per-source live-key
// awareness (e.g. volume live_map filtering) is intentionally out of scope
// and left to a future LiveKeyFilter hook; correctness over a "live"
// optimization for now.
type ReplicationObjectSource struct {
	walker     ObjectWalker
	bucket     string
	prefix     string
	sourceName string
}

// NewReplicationObjectSource constructs a generic replication-object source.
// name identifies it in metrics / Director routing (e.g. "volume",
// "nfs4-replicated"). bucket+prefix bound the walk domain; keyPrefix passed
// to Iter narrows it further when the caller wants to scrub a single subject.
func NewReplicationObjectSource(name, bucket, prefix string, walker ObjectWalker) *ReplicationObjectSource {
	return &ReplicationObjectSource{walker: walker, bucket: bucket, prefix: prefix, sourceName: name}
}

func (s *ReplicationObjectSource) Name() string { return s.sourceName }

func (s *ReplicationObjectSource) Iter(ctx context.Context, scope ScrubScope, keyPrefix string) (<-chan Block, error) {
	_ = scope
	walkPrefix := s.prefix
	if keyPrefix != "" {
		walkPrefix = keyPrefix
	}
	out := make(chan Block, 64)
	go func() {
		defer close(out)
		_ = s.walker.WalkObjects(ctx, s.bucket, walkPrefix, func(obj *storage.Object) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- Block{
				Bucket:       s.bucket,
				Key:          obj.Key,
				VersionID:    "current",
				ExpectedETag: obj.ETag,
				ExpectedSize: obj.Size,
			}:
			}
			return nil
		})
	}()
	return out, nil
}

// ReplicaRepairer is the cluster-side primitive that pulls a healthy peer
// copy and atomically rewrites the local replica. Implemented by
// *cluster.DistributedBackend.RepairReplica.
type ReplicaRepairer interface {
	RepairReplica(ctx context.Context, bucket, key string) error
}

// LocalOpener returns a ReadCloser for the local-only copy of a block. It
// MUST NOT fall back to peers; peer recovery belongs to Repair. Caller
// supplies the opener to keep this package agnostic to backend file layout
// (LocalBackend vs DistributedBackend differ).
type LocalOpener func(bucket, key string) (io.ReadCloser, error)

// ReplicationVerifier MD5s the local copy of each replicated object and
// compares against the expected ETag. Repair delegates to the cluster-side
// ReplicaRepairer.
type ReplicationVerifier struct {
	open     LocalOpener
	repairer ReplicaRepairer
}

// NewReplicationVerifier builds a verifier. open returns the local-only
// bytes for a (bucket, key); rep performs peer-pull repair on detection.
func NewReplicationVerifier(open LocalOpener, rep ReplicaRepairer) *ReplicationVerifier {
	return &ReplicationVerifier{open: open, repairer: rep}
}

func (v *ReplicationVerifier) Verify(ctx context.Context, b Block) (BlockStatus, error) {
	rc, err := v.open(b.Bucket, b.Key)
	if err != nil {
		if os.IsNotExist(err) {
			return BlockStatus{Missing: true, Detail: "local file missing"}, nil
		}
		return BlockStatus{}, fmt.Errorf("open local: %w", err)
	}
	defer rc.Close()
	h := md5.New()
	if _, err := io.Copy(h, rc); err != nil {
		return BlockStatus{}, fmt.Errorf("md5 local: %w", err)
	}
	got := hex.EncodeToString(h.Sum(nil))
	if got != b.ExpectedETag {
		return BlockStatus{Corrupt: true, Detail: fmt.Sprintf("md5 %s != etag %s", got, b.ExpectedETag)}, nil
	}
	return BlockStatus{Healthy: true}, nil
}

func (v *ReplicationVerifier) Repair(ctx context.Context, b Block) error {
	if v.repairer == nil {
		return fmt.Errorf("replication verifier: no replica repairer configured")
	}
	return v.repairer.RepairReplica(ctx, b.Bucket, b.Key)
}
