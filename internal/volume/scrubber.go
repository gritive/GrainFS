package volume

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/storage"
)

// BlockSource implements scrubber.BlockSource for non-EC volume blocks.
// keyPrefix=="" iterates every volume (background); a non-empty prefix
// returned by BlockKeyPrefix(name) narrows iteration to one volume (CLI scrub).
type BlockSource struct {
	mgr *Manager
}

// NewBlockSource constructs a scrubber source over the given Manager.
func NewBlockSource(mgr *Manager) *BlockSource { return &BlockSource{mgr: mgr} }

// Name returns the registry name used by Director / BackgroundScrubber.
func (s *BlockSource) Name() string { return "volume" }

// Iter emits Blocks for every physical volume object that should be verified.
// scope=Full walks the bucket prefix (snapshot-referenced blocks included);
// scope=Live emits only the entries reachable through live_map (no snapshots).
func (s *BlockSource) Iter(ctx context.Context, scope scrubber.ScrubScope, keyPrefix string) (<-chan scrubber.Block, error) {
	out := make(chan scrubber.Block, 64)
	names, err := s.resolveVolumes(keyPrefix)
	if err != nil {
		close(out)
		return out, err
	}
	go func() {
		defer close(out)
		for _, name := range names {
			switch scope {
			case scrubber.ScopeFull:
				s.iterFull(ctx, name, out)
			case scrubber.ScopeLive:
				s.iterLive(ctx, name, out)
			}
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}()
	return out, nil
}

func (s *BlockSource) iterFull(ctx context.Context, name string, out chan<- scrubber.Block) {
	prefix := blockPrefix(name)
	_ = s.mgr.backend.WalkObjects(ctx, volumeBucketName, prefix, func(obj *storage.Object) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- scrubber.Block{
			Bucket:       volumeBucketName,
			Key:          obj.Key,
			VersionID:    "current",
			ExpectedETag: obj.ETag,
			ExpectedSize: obj.Size,
		}:
		}
		return nil
	})
}

func (s *BlockSource) iterLive(ctx context.Context, name string, out chan<- scrubber.Block) {
	s.mgr.mu.Lock()
	lm, err := s.mgr.getLiveMapUnlocked(name)
	s.mgr.mu.Unlock()
	if err != nil {
		return
	}
	if lm == nil {
		// No snapshots → live equals everything under blockPrefix; reuse Full path.
		s.iterFull(ctx, name, out)
		return
	}
	for _, physKey := range lm {
		obj, err := s.mgr.backend.HeadObject(ctx, volumeBucketName, physKey)
		if err != nil {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case out <- scrubber.Block{
			Bucket:       volumeBucketName,
			Key:          physKey,
			VersionID:    "current",
			ExpectedETag: obj.ETag,
			ExpectedSize: obj.Size,
		}:
		}
	}
}

func (s *BlockSource) resolveVolumes(keyPrefix string) ([]string, error) {
	if keyPrefix == "" {
		vs, err := s.mgr.List()
		if err != nil {
			return nil, fmt.Errorf("list volumes: %w", err)
		}
		out := make([]string, len(vs))
		for i, v := range vs {
			out[i] = v.Name
		}
		return out, nil
	}
	if !strings.HasPrefix(keyPrefix, metaPrefix) {
		return nil, fmt.Errorf("volume scrubber: invalid keyPrefix %q (expected %s<name>/blk_)", keyPrefix, metaPrefix)
	}
	rest := strings.TrimPrefix(keyPrefix, metaPrefix)
	rest = strings.TrimSuffix(rest, "/blk_")
	if i := strings.IndexByte(rest, '/'); i >= 0 {
		rest = rest[:i]
	}
	if rest == "" {
		return nil, fmt.Errorf("volume scrubber: empty volume name in keyPrefix")
	}
	return []string{rest}, nil
}

// ReplicaRepairer is the cluster-side primitive volume scrub uses to repair
// non-EC objects. Implemented by *cluster.DistributedBackend.
type ReplicaRepairer interface {
	RepairReplica(ctx context.Context, bucket, key string) error
}

// LocalOpener returns a ReadCloser for the local-only copy of a block. It
// MUST NOT fall back to peers; peer recovery belongs to Repair. The opener
// is supplied by the caller to keep this package agnostic to backend file
// layout (LocalBackend vs DistributedBackend differ).
type LocalOpener func(bucket, key string) (io.ReadCloser, error)

// BlockVerifier MD5s the local copy of each volume block and compares against
// the expected ETag from the backend metadata. Repair delegates to the
// cluster-side ReplicaRepairer.
type BlockVerifier struct {
	open     LocalOpener
	repairer ReplicaRepairer
}

// NewBlockVerifier constructs a verifier. open is responsible for returning
// the local-only bytes for a (bucket, key); rep performs peer-pull repair on
// detection.
func NewBlockVerifier(open LocalOpener, rep ReplicaRepairer) *BlockVerifier {
	return &BlockVerifier{open: open, repairer: rep}
}

// Verify reads the local file, computes MD5, and reports Healthy / Missing /
// Corrupt accordingly. Peer fallback is intentionally NOT used here — that's
// what Repair does.
func (v *BlockVerifier) Verify(ctx context.Context, b scrubber.Block) (scrubber.BlockStatus, error) {
	rc, err := v.open(b.Bucket, b.Key)
	if err != nil {
		if os.IsNotExist(err) {
			return scrubber.BlockStatus{Missing: true, Detail: "local file missing"}, nil
		}
		return scrubber.BlockStatus{}, fmt.Errorf("open local: %w", err)
	}
	defer rc.Close()
	h := md5.New()
	if _, err := io.Copy(h, rc); err != nil {
		return scrubber.BlockStatus{}, fmt.Errorf("md5 local: %w", err)
	}
	got := hex.EncodeToString(h.Sum(nil))
	if got != b.ExpectedETag {
		return scrubber.BlockStatus{Corrupt: true, Detail: fmt.Sprintf("md5 %s != etag %s", got, b.ExpectedETag)}, nil
	}
	return scrubber.BlockStatus{Healthy: true}, nil
}

// Repair pulls a healthy replica from a peer and rewrites the local copy.
func (v *BlockVerifier) Repair(ctx context.Context, b scrubber.Block) error {
	if v.repairer == nil {
		return fmt.Errorf("volume scrub: no replica repairer configured")
	}
	return v.repairer.RepairReplica(ctx, b.Bucket, b.Key)
}
