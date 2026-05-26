package scrubber

import (
	"context"
	"errors"
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

func (s *ReplicationObjectSource) Iter(ctx context.Context, bucket, keyPrefix string) (<-chan Block, error) {
	walkBucket := s.bucket
	if bucket != "" {
		walkBucket = bucket
	}
	walkPrefix := s.prefix
	if keyPrefix != "" {
		walkPrefix = keyPrefix
	}
	out := make(chan Block, 64)
	go func() {
		defer close(out)
		_ = s.walker.WalkObjects(ctx, walkBucket, walkPrefix, func(obj *storage.Object) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- Block{
				Bucket:       walkBucket,
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

// ReplicationVerifier verifies the local copy of each replicated object against
// the stored ETag. Algorithm is selected by ETag length: 32=MD5, 16=xxhash3.
// Repair delegates to the cluster-side ReplicaRepairer.
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
	// Legacy data written before the MD5-oracle restoration carries an empty
	// ETag. Reporting it Corrupt would mass-flag every old block on first
	// deploy and exhaust the repair queue with peers that share the same
	// gap. Mark Skipped so the operator sees the gap without a false alarm.
	if b.ExpectedETag == "" {
		return BlockStatus{Skipped: true, Detail: "no ETag oracle (legacy block)"}, nil
	}
	// ETags we can verify: 32 chars = MD5, 16 chars = xxhash3. Anything else
	// (multipart composite ETags, future formats) cannot be verified locally —
	// skip rather than misreport as corrupt, which would exhaust the repair queue.
	if n := len(b.ExpectedETag); n != 32 && n != 16 {
		return BlockStatus{Skipped: true, Detail: fmt.Sprintf("unrecognized ETag format (len=%d)", n)}, nil
	}
	rc, err := v.open(b.Bucket, b.Key)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return BlockStatus{Missing: true, Detail: "local file missing"}, nil
		}
		return BlockStatus{}, fmt.Errorf("open local: %w", err)
	}
	defer rc.Close()
	match, err := storage.VerifyETag(rc, b.ExpectedETag)
	if err != nil {
		return BlockStatus{}, fmt.Errorf("verify etag local: %w", err)
	}
	if !match {
		return BlockStatus{Corrupt: true, Detail: fmt.Sprintf("etag mismatch: expected %s", b.ExpectedETag)}, nil
	}
	return BlockStatus{Healthy: true}, nil
}

func (v *ReplicationVerifier) Repair(ctx context.Context, b Block) error {
	if v.repairer == nil {
		return fmt.Errorf("replication verifier: no replica repairer configured")
	}
	return v.repairer.RepairReplica(ctx, b.Bucket, b.Key)
}
