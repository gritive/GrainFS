package cluster

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage/eccodec"
)

func dekShardSvc(t *testing.T, root string) (*ShardService, []byte) {
	t.Helper()
	clusterID := bytes.Repeat([]byte{0x42}, 16)
	keeper, err := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0x91}, encrypt.KEKSize), clusterID)
	if err != nil {
		t.Fatalf("NewDEKKeeper: %v", err)
	}
	return NewShardService(root, nil, WithShardDEKKeeper(keeper, clusterID)), clusterID
}

// A cleanable object key ("a/../b") seals under the UNCLEANED ecObjectShardKey;
// the scrubber write+read must bind AAD to that same uncleaned key, not the cleaned
// path — otherwise repair fails AEAD. This is the [P2] regression.
//
// Two independent gates: each reads a shard through a key that is NOT the one the
// shard was sealed under in the RED (path-derived) state, so the AAD/key
// divergence actually surfaces. (The plan's single self-resealing round-trip
// passes in RED — see the deviations note — so it cannot gate the fix.)
func TestScrubberRepairCleanableKeyRoundTrips(t *testing.T) {
	bucket, objKey, versionID, shardIdx := "bkt", "a/../b", "v1", 1
	canonicalKey := ecObjectShardKey(objKey, versionID) // uncleaned: "a/../b/v1"

	// Gate A — survivor-READ of a shard sealed by the ORIGINAL EC write (uncleaned
	// canonical key). No intervening WriteShard, so the read must bind AAD to the
	// uncleaned key. RED: readShardIntegrity derives the cleaned "b/v1" → AEAD fail.
	t.Run("read survivor sealed under uncleaned key", func(t *testing.T) {
		root := t.TempDir()
		svc, _ := dekShardSvc(t, root)
		b := &DistributedBackend{}
		b.SetShardService(svc, []string{"self", "peer-1", "peer-2"})

		orig := bytes.Repeat([]byte("orig"), 64)
		if err := svc.WriteLocalShard(bucket, canonicalKey, shardIdx, orig); err != nil {
			t.Fatalf("WriteLocalShard: %v", err)
		}
		path := filepath.Join(mustShardDir(svc, bucket, canonicalKey, shardIdx), "shard_1")

		res, err := b.ReadShardIntegrity(bucket, objKey, versionID, shardIdx, path)
		if err != nil {
			t.Fatalf("ReadShardIntegrity (cleanable key): %v — AAD/key incoherence", err)
		}
		if !bytes.Equal(res.Payload, orig) {
			t.Fatalf("round-trip mismatch: got %d bytes", len(res.Payload))
		}
	})

	// Gate B — repair-WRITE then read back the way the real object reader does
	// (canonical key). RED: WriteShard seals under the cleaned "b/v1"; the
	// canonical read asks for "a/../b/v1" → AEAD fail.
	t.Run("repaired shard readable via canonical key", func(t *testing.T) {
		root := t.TempDir()
		svc, _ := dekShardSvc(t, root)
		b := &DistributedBackend{}
		b.SetShardService(svc, []string{"self", "peer-1", "peer-2"})

		path := filepath.Join(mustShardDir(svc, bucket, canonicalKey, shardIdx), "shard_1")
		repaired := bytes.Repeat([]byte("repaired"), 64)
		if err := b.WriteShard(bucket, objKey, versionID, shardIdx, path, repaired); err != nil {
			t.Fatalf("WriteShard: %v", err)
		}

		got, err := svc.ReadLocalShard(bucket, canonicalKey, shardIdx)
		if err != nil {
			t.Fatalf("ReadLocalShard via canonical key: %v — AAD/key incoherence", err)
		}
		if !bytes.Equal(got, repaired) {
			t.Fatalf("round-trip mismatch: got %d bytes", len(got))
		}
		// On-disk is GFSENC3 ciphertext, not plaintext.
		raw, _ := os.ReadFile(path)
		if !eccodec.IsEncryptedShard(raw) || bytes.Contains(raw, repaired) {
			t.Fatal("repaired shard is not GFSENC3 ciphertext on disk")
		}
	})
}

// The path-coherence guard (Task 3 Step 1, load-bearing) rejects a WriteShard
// whose path is not the canonical on-disk location for the supplied identity —
// preventing a caller from sealing object A's AAD at object B's path. GREEN-only:
// the guard does not exist in RED, but RED already fails the round-trip gates.
func TestWriteShardRejectsNonCanonicalPath(t *testing.T) {
	root := t.TempDir()
	svc, _ := dekShardSvc(t, root)
	b := &DistributedBackend{}
	b.SetShardService(svc, []string{"self"})

	bucket, objKey, versionID, shardIdx := "bkt", "objA", "v1", 0
	// Canonical path for a DIFFERENT object/identity.
	wrongPath := filepath.Join(mustShardDir(svc, bucket, ecObjectShardKey("objB", versionID), shardIdx), "shard_0")
	if err := b.WriteShard(bucket, objKey, versionID, shardIdx, wrongPath, []byte("x")); err == nil {
		t.Fatal("WriteShard accepted a path that is not the canonical location for the identity")
	}
}

// Read-side fail-closed: a planted plaintext GFSCRC1 shard must be REJECTED on a
// DEK-only service across every read API, not returned as data. ReadLocalShardAt
// and OpenLocalShardRange have their own GFSCRC1 fast-paths (the range-reader is
// the genuine bypasser that does not delegate to ReadLocalShard), so each is
// exercised independently.
func TestDEKReadAPIsRejectPlaintext(t *testing.T) {
	root := t.TempDir()
	svc, _ := dekShardSvc(t, root)
	bucket, key, shardIdx := "bkt", "obj", 0
	dir := mustShardDir(svc, bucket, key, shardIdx)
	if err := svc.ensureShardDir(dir); err != nil {
		t.Fatalf("ensureShardDir: %v", err)
	}
	path := filepath.Join(dir, "shard_0")
	if err := os.WriteFile(path, eccodec.EncodeShard([]byte("plaintext")), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	if _, err := svc.ReadLocalShard(bucket, key, shardIdx); err == nil {
		t.Fatal("ReadLocalShard accepted plaintext GFSCRC1 on a DEK-only service")
	}
	if _, err := svc.OpenLocalShard(bucket, key, shardIdx); err == nil {
		t.Fatal("OpenLocalShard accepted plaintext GFSCRC1 on a DEK-only service")
	}
	buf := make([]byte, 4)
	if _, err := svc.ReadLocalShardAt(bucket, key, shardIdx, 0, buf); err == nil {
		t.Fatal("ReadLocalShardAt accepted plaintext GFSCRC1 on a DEK-only service")
	}
	if _, err := svc.OpenLocalShardRange(bucket, key, shardIdx, 0, 4); err == nil {
		t.Fatal("OpenLocalShardRange accepted plaintext GFSCRC1 on a DEK-only service")
	}
}

// Non-data-dir read fallback must also reject planted plaintext rather than
// returning it as ShardIntegrityUnverifiedLegacy (the read-side plaintext leak).
func TestScrubberNonDataDirReadRejectsPlaintext(t *testing.T) {
	root := t.TempDir()
	svc, _ := dekShardSvc(t, root)
	b := &DistributedBackend{}
	b.SetShardService(svc, []string{"self"})

	// A path under a fresh temp dir is NOT under any shardSvc data dir, so the
	// read routes through the non-data-dir fallback.
	path := filepath.Join(t.TempDir(), "shard_0")
	if err := os.WriteFile(path, eccodec.EncodeShard([]byte("plaintext")), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	if _, err := b.ReadShardIntegrity("bkt", "k", "v1", 0, path); err == nil {
		t.Fatal("ReadShardIntegrity returned planted plaintext from non-data-dir fallback")
	}
}

// A key with enough ".." would make getShardPath resolve outside the shard data
// root. WriteShard/ReadShardIntegrity must refuse it (containment guard restored
// after the shardServiceKeyFromPath removal), not write/read outside the root.
func TestScrubberRejectsTraversalKey(t *testing.T) {
	root := t.TempDir()
	svc, _ := dekShardSvc(t, root)
	b := &DistributedBackend{}
	b.SetShardService(svc, []string{"self"})

	bucket, evilKey, versionID, shardIdx := "bkt", "../../../../etc/escape", "v1", 0
	// The chokepoint itself now rejects the escaping key; WriteShard/
	// ReadShardIntegrity surface that error rather than touching disk, so the
	// supplied path argument is irrelevant.
	if _, err := svc.getShardPath(bucket, ecObjectShardKey(evilKey, versionID), shardIdx); err == nil {
		t.Fatal("getShardPath accepted a traversal key that escapes the shard data root")
	}
	if err := b.WriteShard(bucket, evilKey, versionID, shardIdx, "ignored-path", []byte("x")); err == nil {
		t.Fatal("WriteShard accepted a traversal key that escapes the shard data root")
	}
	if _, err := b.ReadShardIntegrity(bucket, evilKey, versionID, shardIdx, "ignored-path"); err == nil {
		t.Fatal("ReadShardIntegrity accepted a traversal key that escapes the shard data root")
	}
}
