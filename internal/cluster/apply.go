package cluster

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/reservedname"
	"github.com/gritive/GrainFS/internal/storage"
)

// restoreCrashAfterDrop, set only by tests, fires inside FSM.Restore right after DropPrefix
// and before the re-write — simulates a process crash mid-Restore.
var restoreCrashAfterDrop func()

// FSM applies committed Raft log entries to BadgerDB metadata store.
type FSM struct {
	db        MetadataStore
	keys      *stateKeyspace
	clusterID [16]byte
	dekKeeper *encrypt.DEKKeeper

	// Guards onMigrateShard, commitNotifier, and coalesceCfg against concurrent
	// Set* calls + Apply.
	mu sync.RWMutex

	// Optional hooks for Phase 13 balancer. Nil when no peers configured/non-balancer mode.
	// onMigrateShard is a buffered channel; Apply sends non-blocking to avoid blocking the Raft apply loop.
	onMigrateShard chan<- MigrationTask
	commitNotifier interface {
		NotifyCommit(bucket, key, versionID string)
	}
	balancerNotifier interface {
		NotifyMigrationDone(bucket, key, versionID string)
	}

	// coalesceCfg is the FSM's own copy of the coalesce configuration.
	// Protected by mu. Updated via SetCoalesceCfg (called from
	// DistributedBackend.SetCoalesceConfig so the apply loop sees fresh caps).
	coalesceCfg CoalesceConfig

	// fenceLockFn resolves the per-bucket fence RWMutex that the soleauth flip
	// (applySetBucketSoleAuthority) write-locks. Stored in an atomic.Pointer
	// (NOT under mu): ApplyTxn dispatches applySetBucketSoleAuthority WITHOUT
	// holding f.mu, so the apply-side read must be race-free without touching
	// f.mu — taking f.mu there and then the bucket WLock would be a lock-order
	// hazard. Set once at startup via SetFenceLock; nil on unit FSMs without a
	// ShardService (the flip still applies, just unfenced).
	fenceLockFn atomic.Pointer[func(string) *sync.RWMutex]
}

// SetFenceLock wires the per-bucket fence RWMutex accessor (svc.bucketSoleAuthLock)
// so the soleauth flip can take the bucket WRITE-lock. Mirrors SetDEKKeeper as a
// startup setter, but stores into an atomic.Pointer because the apply loop reads
// it WITHOUT holding f.mu. A nil fn clears the hook (flip applies unfenced).
func (f *FSM) SetFenceLock(fn func(string) *sync.RWMutex) {
	if fn == nil {
		f.fenceLockFn.Store(nil)
		return
	}
	f.fenceLockFn.Store(&fn)
}

// SetMigrationHooks wires the FSM to the balancer/migration subsystem.
// ch must be a buffered channel; Apply drops tasks if the channel is full.
// bn (balancerNotifier) is called on CmdMigrationDone to release the inflight slot.
func (f *FSM) SetMigrationHooks(ch chan<- MigrationTask, notifier interface {
	NotifyCommit(bucket, key, versionID string)
}, bn interface {
	NotifyMigrationDone(bucket, key, versionID string)
}) {
	f.mu.Lock()
	f.onMigrateShard = ch
	f.commitNotifier = notifier
	f.balancerNotifier = bn
	f.mu.Unlock()
}

// SetCoalesceCfg updates the FSM's coalesce configuration. Safe to call
// concurrently with Apply; protected by mu.
func (f *FSM) SetCoalesceCfg(cfg CoalesceConfig) {
	f.mu.Lock()
	f.coalesceCfg = cfg
	f.mu.Unlock()
}

// snapshotCoalesceCfg returns a snapshot of the coalesce config under RLock
// to avoid holding the lock during the cap arithmetic in the hot apply path.
func (f *FSM) snapshotCoalesceCfg() CoalesceConfig {
	f.mu.RLock()
	cfg := f.coalesceCfg
	f.mu.RUnlock()
	return cfg
}

// NewFSM creates a new finite state machine backed by BadgerDB.
// If keys is nil, newStateKeyspaceEmpty() is used (single-group identity mode).
func NewFSM(db MetadataStore, keys *stateKeyspace) *FSM {
	if keys == nil {
		keys = newStateKeyspaceEmpty()
	}
	return &FSM{db: db, keys: keys}
}

// ApplyTxn decodes a committed command and applies it to the supplied
// transaction. The caller owns the transaction lifecycle (commit/discard);
// this lets the apply actor batch many entries into one transaction.
func (f *FSM) ApplyTxn(txn MetadataTxn, raw []byte) error {
	cmd, err := DecodeCommand(raw)
	if err != nil {
		return fmt.Errorf("decode command: %w", err)
	}

	switch cmd.Type {
	case CmdNoOp:
		return nil
	case CmdCreateBucket:
		return f.applyCreateBucket(txn, cmd.Data)
	case CmdDeleteBucket:
		return f.applyDeleteBucket(txn, cmd.Data)
	case CmdPutObjectMeta:
		return f.applyPutObjectMeta(txn, cmd.Data)
	case CmdDeleteObject:
		return f.applyDeleteObject(txn, cmd.Data)
	case CmdCreateMultipartUpload:
		return f.applyCreateMultipartUpload(txn, cmd.Data)
	case CmdCompleteMultipart:
		return f.applyCompleteMultipart(txn, cmd.Data)
	case CmdAbortMultipart:
		return f.applyAbortMultipart(txn, cmd.Data)
	case CmdSetBucketPolicy:
		return f.applySetBucketPolicy(txn, cmd.Data)
	case CmdDeleteBucketPolicy:
		return f.applyDeleteBucketPolicy(txn, cmd.Data)
	case CmdMigrateShard:
		return f.applyMigrateShard(txn, cmd.Data)
	case CmdMigrationDone:
		return f.applyMigrationDone(txn, cmd.Data)
	case CmdPutShardPlacement:
		// No-op: shard placement is now derived deterministically from the ring.
		// Old log entries are replayed harmlessly.
		return nil
	case CmdDeleteShardPlacement:
		// No-op: see CmdPutShardPlacement.
		return nil
	case CmdSetRing:
		return f.applySetRing(txn, cmd.Data)
	case CmdDeleteObjectVersion:
		return f.applyDeleteObjectVersion(txn, cmd.Data)
	case CmdSetBucketVersioning:
		return f.applySetBucketVersioning(txn, cmd.Data)
	case CmdSetBucketSoleAuthority:
		return f.applySetBucketSoleAuthority(txn, cmd.Data)
	case CmdSetObjectACL:
		return f.applySetObjectACL(txn, cmd.Data)
	case CmdSetObjectTags:
		return f.applySetObjectTags(txn, cmd.Data)
	case CmdAppendObject:
		return f.applyAppendObjectFromCmd(txn, cmd.Data)
	case CmdCoalesceSegments:
		return f.applyCoalesceSegmentsFromCmd(txn, cmd.Data)
	case CmdPutObjectQuarantine:
		return f.applyPutObjectQuarantine(txn, cmd.Data)
	case CmdResealFSMValues:
		return f.applyResealFSMValues(txn, cmd.Data)
	case CmdFSMValueResealDone:
		// Ordering fence: the marker mutates no state; its only effect is the
		// per-node post-apply hook (notifyOnApply) firing a re-Kick after all
		// preceding CmdResealFSMValues batches in raft order. Gen is carried
		// for logging/observability only — the re-Kick is gen-agnostic. S7-1a-2.
		return nil
	case CmdDeleteMultipartDone:
		return f.applyDeleteMultipartDone(txn, cmd.Data)
	default:
		log.Warn().Uint8("type", uint8(cmd.Type)).Msg("fsm: unknown command type")
		return nil
	}
}

// Apply processes a committed command in its own transaction. Retained for
// offline replay (migrate.go) and tests; the apply actor uses ApplyTxn directly.
func (f *FSM) Apply(raw []byte) error {
	return f.db.Update(func(txn MetadataTxn) error {
		return f.ApplyTxn(txn, raw)
	})
}

func (f *FSM) applyPutObjectQuarantine(txn MetadataTxn, data []byte) error {
	c, err := decodePutObjectQuarantineCmd(data)
	if err != nil {
		return err
	}
	if c.Bucket == "" || c.Key == "" {
		return errors.New("quarantine: bucket and key are required")
	}
	value, err := encodePutObjectQuarantineCmd(c)
	if err != nil {
		return err
	}
	return txn.Set(f.keys.QuarantineKey(c.Bucket, c.Key, c.VersionID), value)
}

// applyResealFSMValues handles CmdResealFSMValues in the serialized apply loop.
// For each key in the command, it reads the CURRENT value from the badger txn
// and reseals it at the KEEPER-CURRENT active gen (via setValue). The skip
// decision compares the value's frame gen to the keeper-current gen — NOT to
// cmd.ActiveGen, which is only a hint (used for the metric label). Tracking
// keeper-current is what prevents a back-to-back-rotation livelock: if a 2nd
// rotation lands mid-drain, this node reseals onto the new current gen and the
// drain's next scan sees the values as already-current.
//
// We deliberately do NOT SealAtGen(cmd.ActiveGen): a follower may have applied
// this data-group command before installing cmd.ActiveGen via meta-raft, so
// SealAtGen would fail closed (ErrDEKGenUnknown). setValue seals at the gen the
// keeper actually holds, which is always resident. Cross-node gen-determinism
// is not required: FSM-value seals are already per-node non-deterministic
// (random nonce), any node can open any node's value while the gen is resident,
// and S7-0 blocks prune so a value never sits at a pruned gen.
//
// The reseal is race-free because it runs in the single-threaded apply loop:
// a concurrent SetBucketPolicy is Raft-ordered before or after, never clobbered.
func (f *FSM) applyResealFSMValues(txn MetadataTxn, data []byte) error {
	c, err := decodeResealFSMValuesCmd(data)
	if err != nil {
		return err
	}
	de := f.dataEncryptor()
	if de == nil {
		return nil // encryption disabled — nothing sealed
	}
	current, ok := f.activeDEKGen()
	if !ok {
		return nil // encryption disabled — nothing sealed
	}
	for _, k := range c.Keys {
		key := []byte(k)
		item, err := txn.Get(key)
		if err == ErrMetaKeyNotFound {
			continue // key deleted since the leader scanned it — fine
		}
		if err != nil {
			return err
		}
		raw, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		gen, ct, frameOK, derr := decodeFSMValueFrameV2(raw)
		if derr != nil {
			// Deliberate fail-closed-and-break: any non-NotFound error (decode,
			// Open, seal) returns, failing the apply → ProposeResealFSMValues
			// errors → DrainFSMValueRewrap returns and the single-flight guard
			// releases via its defer. We do NOT skip-and-continue: that would
			// re-livelock the drain on a persistently-stale key.
			return derr
		}
		if !frameOK || gen == current {
			continue // plaintext or already keeper-current — idempotent skip
		}
		plain, err := de.Open(encrypt.DomainFSMValue, []encrypt.AADField{encrypt.FieldBytes(key)}, gen, ct)
		if err != nil {
			return err // fail-closed-and-break (see above)
		}
		if err := f.setValue(txn, key, plain); err != nil { // reseals at keeper-current gen
			return err // fail-closed-and-break (see above)
		}
		// Label with the actual reseal target (keeper-current), not cmd.ActiveGen
		// (which is only a hint of the rotation that drove this batch and can lag
		// keeper-current under back-to-back rotations).
		RewrapFSMValuesTotal.WithLabelValues(strconv.FormatUint(uint64(current), 10)).Inc()
	}
	return nil
}

// deleteMarkerETag is the sentinel ETag we store on a tombstone (soft-delete).
// Used by callers to distinguish a real object version from a delete marker.
const deleteMarkerETag = "DEL"

func (f *FSM) applyCreateBucket(txn MetadataTxn, data []byte) error {
	c, err := decodeCreateBucketCmd(data)
	if err != nil {
		return err
	}
	if !c.BypassReserved && reservedname.IsReservedBucketName(c.Bucket) {
		return fmt.Errorf("bucket name %q is reserved and cannot be created via public API", c.Bucket)
	}
	return txn.Set(f.keys.BucketKey(c.Bucket), []byte(`{}`))
}

func (f *FSM) applyDeleteBucket(txn MetadataTxn, data []byte) error {
	c, err := decodeDeleteBucketCmd(data)
	if err != nil {
		return err
	}
	if reservedname.IsReservedBucketName(c.Bucket) {
		return fmt.Errorf("bucket name %q is reserved and cannot be deleted via public API", c.Bucket)
	}
	// Clear the per-bucket state so a recreated same-name bucket starts fresh (S3
	// semantics — a new bucket inherits nothing from a prior incarnation): the
	// existence record, the bucket policy, the versioning state, and the soleauth
	// STATE (so a deleted soleauth=on bucket does not come back stuck in terminal
	// `on`). DELIBERATELY does NOT clear soleauthepoch:{b}: that epoch is a
	// monotonic cross-incarnation floor — resetting it would let a dead
	// incarnation's stale forwarded write pass the soleauth fence on the recreated
	// bucket. Tolerate absent keys (most are unset on a plain bucket), mirroring
	// applyDeleteObject. (Per-bucket state in other subsystems — lifecycle config,
	// IAM bucket-upstream — lives outside this FSM keyspace and is tracked
	// separately; object obj:/lat: records are GC'd by the orphan scrubber and
	// DeleteBucket requires an empty bucket.)
	for _, key := range [][]byte{
		f.keys.BucketKey(c.Bucket),
		f.keys.BucketPolicyKey(c.Bucket),
		f.keys.BucketVerKey(c.Bucket),
		f.keys.BucketSoleAuthKey(c.Bucket),
	} {
		if err := txn.Delete(key); err != nil && err != ErrMetaKeyNotFound {
			return err
		}
	}
	return nil
}

func (f *FSM) applyPutObjectMeta(txn MetadataTxn, data []byte) error {
	c, err := decodePutObjectMetaCmd(data)
	if err != nil {
		return err
	}
	metaObj := buildPutObjectMeta(c)
	if err := f.checkPutObjectExpectedETag(txn, c.Bucket, c.Key, c.ExpectedETag); err != nil {
		return err
	}
	return f.persistPutObjectMetaUpdate(txn, c, metaObj)
}

func (f *FSM) applyDeleteObject(txn MetadataTxn, data []byte) error {
	c, err := decodeDeleteObjectCmd(data)
	if err != nil {
		return err
	}
	// VersionID == "" is a legacy DeleteObjectCmd (no version ID attached by the
	// proposer). Treat it as a hard delete of the legacy latest-only records —
	// preserves prior semantics for pre-versioning log replay.
	if c.VersionID == "" {
		// Validate (decrypt) the meta before deleting; surfaces corruption errors.
		if item, gerr := txn.Get(f.keys.ObjectMetaKey(c.Bucket, c.Key)); gerr == nil {
			if verr := item.Value(func(raw []byte) error {
				_, err := f.openValue(item.Key(), raw)
				return err
			}); verr != nil {
				return verr
			}
		}
		if err := txn.Delete(f.keys.ObjectMetaKey(c.Bucket, c.Key)); err != nil && err != ErrMetaKeyNotFound {
			return err
		}
		if err := txn.Delete(f.keys.ShardPlacementKey(c.Bucket, c.Key)); err != nil && err != ErrMetaKeyNotFound {
			return err
		}
		return nil
	}

	// Tombstone (soft-delete): write a delete marker as a new version and
	// flip the latest pointer at it. Prior versions remain addressable via
	// GetObjectVersion.
	markerMeta, err := marshalObjectMeta(objectMeta{
		Key:          c.Key,
		Size:         0,
		ContentType:  "",
		ETag:         deleteMarkerETag,
		LastModified: 0,
	})
	if err != nil {
		return fmt.Errorf("marshal delete marker: %w", err)
	}
	// Read the current latest versionID before overwriting it, so we can
	// delete the versioned placement record that was stored under
	// shardPlacementKey(bucket, key+"/"+prevVersionID).
	prevVersionID := ""
	if latItem, gerr := txn.Get(f.keys.LatestKey(c.Bucket, c.Key)); gerr == nil {
		_ = latItem.Value(func(v []byte) error { prevVersionID = string(v); return nil })
	}

	if err := f.setValue(txn, f.keys.ObjectMetaKeyV(c.Bucket, c.Key, c.VersionID), markerMeta); err != nil {
		return err
	}
	if err := txn.Set(f.keys.LatestKey(c.Bucket, c.Key), []byte(c.VersionID)); err != nil {
		return err
	}
	// Legacy latest-only key is removed so HeadObject returns 404 while prior
	// versions remain queryable via GetObjectVersion.
	if err := txn.Delete(f.keys.ObjectMetaKey(c.Bucket, c.Key)); err != nil && err != ErrMetaKeyNotFound {
		return err
	}
	// Remove the versioned placement record for the previous latest version.
	// Bare-key placement (legacy pre-versioned objects) is also cleaned up.
	if prevVersionID != "" {
		placementKey := f.keys.ShardPlacementKey(c.Bucket, c.Key+"/"+prevVersionID)
		if err := txn.Delete(placementKey); err != nil && err != ErrMetaKeyNotFound {
			return err
		}
	}
	if err := txn.Delete(f.keys.ShardPlacementKey(c.Bucket, c.Key)); err != nil && err != ErrMetaKeyNotFound {
		return err
	}
	return nil
}

// applyDeleteObjectVersion hard-deletes one specific version and, if it was the
// latest, recomputes the latest pointer from the remaining versions (ULIDs sort
// lexicographically by time, so the max-sorted key wins).
func (f *FSM) applyDeleteObjectVersion(txn MetadataTxn, data []byte) error {
	c, err := decodeDeleteObjectVersionCmd(data)
	if err != nil {
		return err
	}
	metaKey := f.keys.ObjectMetaKeyV(c.Bucket, c.Key, c.VersionID)
	latKey := f.keys.LatestKey(c.Bucket, c.Key)

	if _, gerr := txn.Get(metaKey); gerr == ErrMetaKeyNotFound {
		return nil // idempotent
	} else if gerr != nil {
		return gerr
	}
	if derr := txn.Delete(metaKey); derr != nil {
		return derr
	}
	// Remove the versioned placement record stored under key+"/"+versionID.
	placementKey := f.keys.ShardPlacementKey(c.Bucket, c.Key+"/"+c.VersionID)
	if derr := txn.Delete(placementKey); derr != nil && derr != ErrMetaKeyNotFound {
		return derr
	}

	// If the deleted version was the latest, find the new latest by scanning
	// remaining versioned keys and picking the max (ULID sorts by time ASC).
	if latItem, gerr := txn.Get(latKey); gerr == nil {
		var current string
		_ = latItem.Value(func(v []byte) error { current = string(v); return nil })
		if current == c.VersionID {
			newLatest := ""
			rawPrefix := []byte("obj:" + c.Bucket + "/" + c.Key + "/")
			if serr := f.keys.scanGroupPrefix(txn, rawPrefix, func(raw []byte, _ MetaItem) error {
				vid := string(raw[len(rawPrefix):])
				if vid == "" || vid == c.VersionID {
					return nil
				}
				if vid > newLatest {
					newLatest = vid
				}
				return nil
			}); serr != nil {
				return serr
			}
			if newLatest == "" {
				if derr := txn.Delete(latKey); derr != nil && derr != ErrMetaKeyNotFound {
					return derr
				}
				// No versions left — drop legacy latest-only key as well.
				if derr := txn.Delete(f.keys.ObjectMetaKey(c.Bucket, c.Key)); derr != nil && derr != ErrMetaKeyNotFound {
					return derr
				}
			} else {
				if derr := txn.Set(latKey, []byte(newLatest)); derr != nil {
					return derr
				}
			}
		}
	} else if gerr != ErrMetaKeyNotFound {
		return gerr
	}
	return nil
}

func (f *FSM) applyCreateMultipartUpload(txn MetadataTxn, data []byte) error {
	c, err := decodeCreateMultipartUploadCmd(data)
	if err != nil {
		return err
	}
	meta, err := marshalClusterMultipartMeta(clusterMultipartMeta{
		Bucket:           c.Bucket,
		Key:              c.Key,
		CreatedAt:        c.CreatedAt,
		ContentType:      c.ContentType,
		PlacementGroupID: c.PlacementGroupID,
		Tags:             c.Tags,
	})
	if err != nil {
		return fmt.Errorf("marshal multipart meta: %w", err)
	}
	return f.setValue(txn, f.keys.MultipartKey(c.UploadID), meta)
}

// applyCompleteMultipart is intentionally on data_raft (Phase 3 boundary): it
// atomically writes object meta and deletes the multipart manifest key in one
// BadgerDB txn. Splitting the two operations would violate that atomicity.
// Reads fall back to this BadgerDB entry when quorum meta is absent (headObjectMeta).
func (f *FSM) applyCompleteMultipart(txn MetadataTxn, data []byte) error {
	c, err := decodeCompleteMultipartCmd(data)
	if err != nil {
		return err
	}
	// Blob-authoritative multipart (versioning-enabled): the proposer built the
	// completed object's encoded PutObjectMetaCmd and passed it as meta_blob. In
	// this mode the per-version quorum-meta blob (written by the proposer after this
	// commit, from the done-marker) is the SOLE AUTHORITY — apply writes NO FSM
	// obj:/lat: record, only the done-marker carrying meta_blob. Non-versioned /
	// Suspended completes carry no meta_blob and keep the legacy FSM obj:/lat: write
	// (non-versioned blob authority is a separate task).
	blobAuthoritative := len(c.MetaBlob) > 0
	mpuKey := f.keys.MultipartKey(c.UploadID)
	item, err := txn.Get(mpuKey)
	if err == ErrMetaKeyNotFound {
		// Manifest absent: check for a done marker written by a prior commit.
		mb, merr := txn.Get(f.keys.MultipartDoneKey(c.UploadID))
		if merr == ErrMetaKeyNotFound {
			return storage.ErrUploadNotFound
		}
		if merr != nil {
			return fmt.Errorf("get multipart done marker: %w", merr)
		}
		raw, merr := f.itemValueCopy(mb)
		if merr != nil {
			return fmt.Errorf("read multipart done marker: %w", merr)
		}
		marker, merr := unmarshalMultipartDone(raw)
		if merr != nil {
			return fmt.Errorf("unmarshal multipart done marker: %w", merr)
		}
		if marker.Bucket == c.Bucket && marker.Key == c.Key {
			return nil // idempotent — prior commit won
		}
		return fmt.Errorf("complete multipart upload id %s already completed for %s/%s, got %s/%s",
			c.UploadID, marker.Bucket, marker.Key, c.Bucket, c.Key)
	}
	if err != nil {
		return err
	}
	raw, err := f.itemValueCopy(item)
	if err != nil {
		return err
	}
	mpu, err := unmarshalClusterMultipartMeta(raw)
	if err != nil {
		return err
	}
	if mpu.Bucket != c.Bucket || mpu.Key != c.Key {
		return fmt.Errorf("complete multipart upload mismatch: upload %s is for %s/%s, got %s/%s", c.UploadID, mpu.Bucket, mpu.Key, c.Bucket, c.Key)
	}

	// Legacy FSM write (non-versioned/Suspended only). Blob-authoritative completes
	// skip this entirely — their per-version blob is the sole authority.
	if !blobAuthoritative {
		objMeta, merr := marshalObjectMeta(objectMeta{
			Key:              c.Key,
			Size:             c.Size,
			ContentType:      c.ContentType,
			ETag:             c.ETag,
			LastModified:     c.ModTime,
			ECData:           c.ECData,
			ECParity:         c.ECParity,
			NodeIDs:          c.NodeIDs,
			PlacementGroupID: c.PlacementGroupID,
			Parts:            c.Parts,
			Segments:         segmentMetaEntriesToRefs(c.Segments),
			Tags:             c.Tags,
		})
		if merr != nil {
			return fmt.Errorf("marshal object meta: %w", merr)
		}
		// Dual-write legacy + versioned, same pattern as applyPutObjectMeta.
		if err := f.setValue(txn, f.keys.ObjectMetaKey(c.Bucket, c.Key), objMeta); err != nil {
			return err
		}
		if c.VersionID != "" {
			if err := f.setValue(txn, f.keys.ObjectMetaKeyV(c.Bucket, c.Key, c.VersionID), objMeta); err != nil {
				return err
			}
			if err := txn.Set(f.keys.LatestKey(c.Bucket, c.Key), []byte(c.VersionID)); err != nil {
				return err
			}
		}
	}
	// Write a done marker so that a retried apply of the same command is idempotent.
	// For blob-authoritative completes the marker carries meta_blob so any retry /
	// concurrent loser re-writes the winner's per-version blob from it.
	doneMarker := multipartDone{
		UploadID:  c.UploadID,
		Bucket:    c.Bucket,
		Key:       c.Key,
		VersionID: c.VersionID,
		ModTime:   c.ModTime,
		MetaBlob:  c.MetaBlob,
	}
	doneBytes, err := marshalMultipartDone(doneMarker)
	if err != nil {
		return fmt.Errorf("marshal multipart done marker: %w", err)
	}
	// mpudone marker is retained until GC sweeps it (S4c-0 PR2 Task 5) — it must outlive the client retry window.
	if err := f.setValue(txn, f.keys.MultipartDoneKey(c.UploadID), doneBytes); err != nil {
		return fmt.Errorf("set multipart done marker: %w", err)
	}
	return txn.Delete(mpuKey)
}

func (f *FSM) applyAbortMultipart(txn MetadataTxn, data []byte) error {
	c, err := decodeAbortMultipartCmd(data)
	if err != nil {
		return err
	}
	err = txn.Delete(f.keys.MultipartKey(c.UploadID))
	if err == ErrMetaKeyNotFound {
		return nil
	}
	return err
}

// applyDeleteMultipartDone handles CmdDeleteMultipartDone: it batch-deletes
// mpudone idempotency markers for the listed upload IDs.  Missing markers are
// silently ignored so the command is idempotent across retries.
func (f *FSM) applyDeleteMultipartDone(txn MetadataTxn, data []byte) error {
	c, err := decodeDeleteMultipartDoneCmd(data)
	if err != nil {
		return err
	}
	for _, id := range c.UploadIDs {
		if derr := txn.Delete(f.keys.MultipartDoneKey(id)); derr != nil && derr != ErrMetaKeyNotFound {
			return derr
		}
	}
	return nil
}

func (f *FSM) applySetBucketPolicy(txn MetadataTxn, data []byte) error {
	c, err := decodeSetBucketPolicyCmd(data)
	if err != nil {
		return err
	}
	policyJSON := append([]byte(nil), c.PolicyJSON...)
	return f.setValue(txn, f.keys.BucketPolicyKey(c.Bucket), policyJSON)
}

func (f *FSM) applyDeleteBucketPolicy(txn MetadataTxn, data []byte) error {
	c, err := decodeDeleteBucketPolicyCmd(data)
	if err != nil {
		return err
	}
	err = txn.Delete(f.keys.BucketPolicyKey(c.Bucket))
	if err == ErrMetaKeyNotFound {
		return nil
	}
	return err
}

func (f *FSM) applySetBucketVersioning(txn MetadataTxn, data []byte) error {
	c, err := decodeSetBucketVersioningCmd(data)
	if err != nil {
		return err
	}
	if _, err := txn.Get(f.keys.BucketKey(c.Bucket)); err == ErrMetaKeyNotFound {
		return storage.ErrBucketNotFound
	} else if err != nil {
		return err
	}
	return txn.Set(f.keys.BucketVerKey(c.Bucket), []byte(c.State))
}

// applySetBucketSoleAuthority handles CmdSetBucketSoleAuthority: it enforces
// the one-way tri-state guard (off→pending→on; pending→off abort; on terminal)
// and writes the soleauth state raw to BucketSoleAuthKey — mirroring
// applySetBucketVersioning which also writes the versioning state raw.
func (f *FSM) applySetBucketSoleAuthority(txn MetadataTxn, data []byte) error {
	c, err := decodeSetBucketSoleAuthorityCmd(data)
	if err != nil {
		return err
	}
	if _, err := txn.Get(f.keys.BucketKey(c.Bucket)); err == ErrMetaKeyNotFound {
		return storage.ErrBucketNotFound
	} else if err != nil {
		return err
	}
	if c.State != soleAuthOff && c.State != soleAuthPending && c.State != soleAuthOn {
		return fmt.Errorf("invalid soleauth state %q", c.State)
	}
	// Fence the mutation region under the per-bucket WRITE-lock so a later flip
	// fences the quorum-meta leaves' READ-lock (Task 4). Read the accessor
	// race-free from the atomic.Pointer (ApplyTxn dispatches us WITHOUT f.mu;
	// taking f.mu here then the bucket WLock would be a lock-order hazard). Nil
	// on unit FSMs without a ShardService — the flip still applies, unfenced.
	if p := f.fenceLockFn.Load(); p != nil {
		mu := (*p)(c.Bucket)
		mu.Lock()
		defer mu.Unlock()
	}
	cur := soleAuthOff
	if item, gerr := txn.Get(f.keys.BucketSoleAuthKey(c.Bucket)); gerr == nil {
		raw, e := item.ValueCopy(nil)
		if e != nil {
			return e
		}
		cur = string(raw)
	} else if gerr != ErrMetaKeyNotFound {
		return gerr
	}
	if !soleAuthTransitionAllowed(cur, c.State) {
		return fmt.Errorf("soleauth transition refused for %s: %s -> %s (one-way off->pending->on; pending->off abort; on terminal)", c.Bucket, cur, c.State)
	}
	// Bump the monotonic epoch on a REAL state transition; idempotent same-state
	// writes keep the epoch unchanged. FSM apply is single-goroutine and
	// replicated, so the bump is identical on every node.
	curEpoch := uint32(0)
	if item, gerr := txn.Get(f.keys.BucketSoleAuthEpochKey(c.Bucket)); gerr == nil {
		raw, e := item.ValueCopy(nil)
		if e != nil {
			return e
		}
		curEpoch = decodeSoleAuthEpoch(raw)
	} else if gerr != ErrMetaKeyNotFound {
		return gerr
	}
	newEpoch := curEpoch
	if cur != c.State {
		newEpoch = curEpoch + 1
	}
	// Apply the monotonic floor from snapshot-restore: raises the epoch to
	// max(computed, EpochFloor) without modifying the committed state. This repairs
	// the fidelity gap where a pending↔off cycle accumulated epoch bumps that the
	// transition-replay alone cannot reproduce, preventing stale wire epochs.
	if c.EpochFloor > newEpoch {
		newEpoch = c.EpochFloor
	}
	if err := txn.Set(f.keys.BucketSoleAuthEpochKey(c.Bucket), encodeSoleAuthEpoch(newEpoch)); err != nil {
		return err
	}
	return txn.Set(f.keys.BucketSoleAuthKey(c.Bucket), []byte(c.State))
}

func (f *FSM) applySetObjectACL(txn MetadataTxn, data []byte) error {
	c, err := decodeSetObjectACLCmd(data)
	if err != nil {
		return err
	}
	legacyKey := f.keys.ObjectMetaKey(c.Bucket, c.Key)
	item, err := txn.Get(legacyKey)
	if err == ErrMetaKeyNotFound {
		return storage.ErrObjectNotFound
	}
	if err != nil {
		return err
	}
	return item.Value(func(raw []byte) error {
		val, err := f.openValue(item.Key(), raw)
		if err != nil {
			return err
		}
		m, merr := unmarshalObjectMeta(val)
		if merr != nil {
			return merr
		}
		m.ACL = c.ACL
		newVal, merr := marshalObjectMeta(m)
		if merr != nil {
			return merr
		}
		if err := f.setValue(txn, legacyKey, newVal); err != nil {
			return err
		}
		// Also update the versioned record if this bucket has versioning enabled.
		latItem, lerr := txn.Get(f.keys.LatestKey(c.Bucket, c.Key))
		if lerr != nil {
			return nil //nolint:nilerr // no versioned key, nothing more to do
		}
		var versionID string
		_ = latItem.Value(func(v []byte) error { versionID = string(v); return nil })
		if versionID == "" {
			return nil
		}
		vKey := f.keys.ObjectMetaKeyV(c.Bucket, c.Key, versionID)
		vItem, verr := txn.Get(vKey)
		if verr != nil {
			return nil //nolint:nilerr // versioned record missing, skip
		}
		return vItem.Value(func(raw []byte) error {
			vval, err := f.openValue(vItem.Key(), raw)
			if err != nil {
				return err
			}
			vm, merr := unmarshalObjectMeta(vval)
			if merr != nil {
				return merr
			}
			vm.ACL = c.ACL
			vNewVal, merr := marshalObjectMeta(vm)
			if merr != nil {
				return merr
			}
			return f.setValue(txn, vKey, vNewVal)
		})
	})
}

func (f *FSM) applySetObjectTags(txn MetadataTxn, data []byte) error {
	c, err := decodeSetObjectTagsCmd(data)
	if err != nil {
		return err
	}
	if c.VersionID != "" {
		vKey := f.keys.ObjectMetaKeyV(c.Bucket, c.Key, c.VersionID)
		if err := mutateVersionTags(f, txn, vKey, c.Tags); err != nil {
			return err
		}
		latItem, lerr := txn.Get(f.keys.LatestKey(c.Bucket, c.Key))
		if lerr != nil {
			return nil //nolint:nilerr // no latest marker, so no legacy-current mirror to update
		}
		var latest string
		if err := latItem.Value(func(v []byte) error {
			latest = string(v)
			return nil
		}); err != nil {
			return err
		}
		if latest != c.VersionID {
			return nil
		}
		return mutateVersionTags(f, txn, f.keys.ObjectMetaKey(c.Bucket, c.Key), c.Tags)
	}
	// VersionID == "" → mutate legacy (current) record, and if LatestKey
	// exists also dual-write the latest versioned record.
	legacyKey := f.keys.ObjectMetaKey(c.Bucket, c.Key)
	if err := mutateVersionTags(f, txn, legacyKey, c.Tags); err != nil {
		return err
	}
	// Best-effort dual-write to latest versioned record (mirrors SetObjectACL semantics).
	latItem, lerr := txn.Get(f.keys.LatestKey(c.Bucket, c.Key))
	if lerr != nil {
		return nil //nolint:nilerr // no versioning, nothing more to do
	}
	var versionID string
	_ = latItem.Value(func(v []byte) error { versionID = string(v); return nil })
	if versionID == "" {
		return nil
	}
	vKey := f.keys.ObjectMetaKeyV(c.Bucket, c.Key, versionID)
	return mutateVersionTags(f, txn, vKey, c.Tags)
}

// mutateVersionTags reads the objectMeta at key, replaces .Tags, writes back.
// Returns storage.ErrObjectNotFound if the key doesn't exist.
func mutateVersionTags(f *FSM, txn MetadataTxn, key []byte, tags []storage.Tag) error {
	item, err := txn.Get(key)
	if err == ErrMetaKeyNotFound {
		return storage.ErrObjectNotFound
	}
	if err != nil {
		return err
	}
	return item.Value(func(raw []byte) error {
		val, err := f.openValue(item.Key(), raw)
		if err != nil {
			return err
		}
		m, merr := unmarshalObjectMeta(val)
		if merr != nil {
			return merr
		}
		m.Tags = append([]storage.Tag(nil), tags...) // defensive copy
		newVal, merr := marshalObjectMeta(m)
		if merr != nil {
			return merr
		}
		return f.setValue(txn, key, newVal)
	})
}

// applyAppendObjectFromCmd handles a CmdAppendObject Raft entry. Mirrors the
// applySetObjectACL read-mutate-write pattern: read existing objectMeta →
// validate append invariants → write updated objectMeta with the new segment.
//
// Idempotent on replay: if the segment's BlobID already appears in
// existing.Segments (replay of an already-applied entry), this is a no-op.
//
// When cmd.VersionID is non-empty (the normal AppendObject path), apply
// dual-writes the versioned key + LatestKey alongside the legacy
// ObjectMetaKey — matching applyPutObjectMeta — so that headObjectMeta returns
// obj.VersionID populated and downstream commitObjectIndex can propose a
// valid MetaPutObjectIndex entry (which rejects empty version_id).
//
// When cmd.VersionID is empty (legacy Raft replay or direct apply-test
// fixtures), only the legacy ObjectMetaKey is written to preserve prior
// semantics.
func (f *FSM) applyAppendObjectFromCmd(txn MetadataTxn, data []byte) error {
	cmd, err := decodeAppendObjectCmd(data)
	if err != nil {
		return fmt.Errorf("decode AppendObjectCmd: %w", err)
	}

	// Snapshot config once; avoids holding mu across the BadgerDB transaction.
	coalesceCfg := f.snapshotCoalesceCfg()
	modifiedUnixSec := cmd.ModifiedUnixSec
	if modifiedUnixSec == 0 {
		modifiedUnixSec = time.Now().Unix()
	}

	resolved, err := f.resolveObjectMetaForAppendUpdate(txn, cmd.Bucket, cmd.Key, cmd.BlobID)
	if err != nil {
		return err
	}
	if resolved.AlreadyApplied {
		return nil
	}

	updated, result, err := applyAppendObjectTransition(appendObjectTransitionInput{
		Existing:          resolved.Existing,
		ExistingVersionID: resolved.ExistingVersionID,
		Cmd:               cmd,
		ModifiedUnixSec:   modifiedUnixSec,
		CoalesceCfg:       coalesceCfg,
	})
	if result.Noop {
		return nil
	}
	if result.SizeCapRejected {
		metrics.AppendSizeCapRejectedTotal.Inc()
	}
	if err != nil {
		return err
	}

	return f.persistObjectMetaUpdate(txn, objectMetaPersistenceInput{
		Bucket:    cmd.Bucket,
		Key:       cmd.Key,
		VersionID: cmd.VersionID,
		Meta:      updated,
		Policy:    objectMetaPersistencePublishLatest,
	})
}

func appendBaseCoalescedRef(key, versionID string, existing *objectMeta) CoalescedShardRef {
	coalescedID := "base"
	if versionID != "" {
		coalescedID = "base-" + versionID
	}
	return CoalescedShardRef{
		CoalescedID: coalescedID,
		Size:        existing.Size,
		ETag:        existing.ETag,
		ShardKey:    ecObjectShardKey(key, versionID),
		ECData:      existing.ECData,
		ECParity:    existing.ECParity,
		StripeBytes: existing.StripeBytes,
		NodeIDs:     append([]string(nil), existing.NodeIDs...),
	}
}

// applyCoalesceSegmentsFromCmd handles a CmdCoalesceSegments Raft entry. The
// command consumes a prefix of objectMeta.Segments and appends a single
// CoalescedShardRef. Read-modify-write pattern mirroring
// applyAppendObjectFromCmd.
//
// Idempotency rules (replay-safe):
//   - If CoalescedID already present in objectMeta.Coalesced → no-op.
//   - ConsumedSegmentIDs are removed by exact BlobID match; missing IDs
//     are skipped (a concurrent append that arrived between snapshot and
//     apply is preserved).
//   - Size/ETag of objectMeta are NOT modified: a coalesce is metadata
//     reorganization, not a content change.
func (f *FSM) applyCoalesceSegmentsFromCmd(txn MetadataTxn, data []byte) error {
	cmd, err := decodeCoalesceSegmentsCmd(data)
	if err != nil {
		return fmt.Errorf("decode CoalesceSegmentsCmd: %w", err)
	}
	if cmd.CoalescedID == "" || cmd.ShardKey == "" {
		return fmt.Errorf("coalesce: empty CoalescedID or ShardKey")
	}

	resolved, err := f.resolveObjectMetaForCoalesceUpdate(txn, cmd.Bucket, cmd.Key)
	if err != nil {
		return err
	}
	if !resolved.Found {
		return nil // object deleted concurrently — drop silently
	}

	updated, result, err := applyCoalesceSegmentsTransition(resolved.Meta, cmd)
	if result.Noop {
		return nil
	}
	if result.CoalescedEntriesAtCap {
		metrics.AppendCoalescedEntriesAtCap.Inc()
	}
	if err != nil {
		return err
	}
	return f.persistObjectMetaUpdate(txn, objectMetaPersistenceInput{
		Bucket:    cmd.Bucket,
		Key:       cmd.Key,
		VersionID: resolved.VersionID,
		Meta:      updated,
		Policy:    objectMetaPersistenceMirrorVersion,
	})
}

// pendingMigrationKey returns the BadgerDB key for a not-yet-executed migration task.
//
//nolint:unused // referenced by apply_test.go.
func pendingMigrationKey(bucket, key, versionID string) []byte {
	return []byte("pending-migration:" + bucket + "/" + key + "/" + versionID)
}

// persistPendingMigration writes a migration task to the supplied transaction
// so it survives a crash. Shares the apply actor's batch transaction.
func (f *FSM) persistPendingMigration(txn MetadataTxn, task MigrationTask) error {
	val, err := encodeMigrateShardCmd(MigrateShardFSMCmd(task))
	if err != nil {
		return fmt.Errorf("fsm: marshal pending migration: %w", err)
	}
	return txn.Set(f.keys.PendingMigrationKey(task.Bucket, task.Key, task.VersionID), val)
}

// RecoverPending reads all pending-migration keys from BadgerDB and enqueues them
// to ch for re-execution. Call this at startup AFTER starting the executor goroutine
// so the channel consumer is running before tasks are sent.
func (f *FSM) RecoverPending(ctx context.Context, ch chan<- MigrationTask) error {
	return f.db.View(func(txn MetadataTxn) error {
		return f.keys.scanGroupPrefix(txn, []byte("pending-migration:"), func(_ []byte, item MetaItem) error {
			var taskData []byte
			if err := item.Value(func(val []byte) error {
				taskData = make([]byte, len(val))
				copy(taskData, val)
				return nil
			}); err != nil {
				log.Error().Err(err).Msg("fsm: recover pending migration: read failed")
				return nil
			}
			cmd, err := decodeMigrateShardCmd(taskData)
			if err != nil {
				log.Error().Err(err).Msg("fsm: recover pending migration: decode failed")
				return nil
			}
			task := MigrationTask(cmd)
			select {
			case ch <- task:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		})
	})
}

func (f *FSM) applyMigrateShard(txn MetadataTxn, data []byte) error {
	cmd, err := decodeMigrateShardCmd(data)
	if err != nil {
		return fmt.Errorf("decode MigrateShardCmd: %w", err)
	}
	// Guard: balancer proposals without object identity are placeholders; skip them.
	if cmd.Bucket == "" || cmd.Key == "" {
		return nil
	}
	task := MigrationTask(cmd)
	f.mu.RLock()
	ch := f.onMigrateShard
	f.mu.RUnlock()
	if ch != nil {
		select {
		case ch <- task:
		default:
			// Channel full: persist to BadgerDB so the task survives a crash.
			if err := f.persistPendingMigration(txn, task); err != nil {
				log.Error().Str("bucket", task.Bucket).Str("key", task.Key).Err(err).Msg("fsm: migration queue full and persist failed — task lost")
			} else {
				log.Warn().Str("bucket", task.Bucket).Str("key", task.Key).Msg("fsm: migration queue full, persisted to BadgerDB")
			}
		}
	}
	return nil
}

func (f *FSM) applyMigrationDone(txn MetadataTxn, data []byte) error {
	cmd, err := decodeMigrationDoneCmd(data)
	if err != nil {
		return fmt.Errorf("decode MigrationDoneCmd: %w", err)
	}
	// Clean up any persisted pending-migration entry for this task using the
	// shared batch transaction.
	if derr := txn.Delete(f.keys.PendingMigrationKey(cmd.Bucket, cmd.Key, cmd.VersionID)); derr != nil && derr != ErrMetaKeyNotFound {
		log.Warn().Str("bucket", cmd.Bucket).Str("key", cmd.Key).Err(derr).Msg("fsm: delete pending-migration key failed")
	}
	f.mu.RLock()
	notifier := f.commitNotifier
	bn := f.balancerNotifier
	f.mu.RUnlock()
	if notifier != nil {
		notifier.NotifyCommit(cmd.Bucket, cmd.Key, cmd.VersionID)
	}
	if bn != nil {
		bn.NotifyMigrationDone(cmd.Bucket, cmd.Key, cmd.VersionID)
	}
	return nil
}

// Snapshot serializes this group's metadata state for Raft snapshots. Keys in
// the snapshot blob are GROUP-RELATIVE (the group prefix is stripped); Restore
// re-applies the prefix on write. For the empty keyspace the group prefix is nil,
// so this is a whole-DB scan exactly as before.
func (f *FSM) Snapshot() ([]byte, error) {
	state := make(map[string][]byte)
	err := f.db.View(func(txn MetadataTxn) error {
		return f.keys.scanGroupPrefix(txn, nil, func(rawKey []byte, item MetaItem) error {
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			state[string(rawKey)] = val
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return marshalSnapshotState(state)
}

// dropAllKeys deletes every key in the underlying DB. Used by Restore's
// empty-keyspace branch (single-group / pre-shared mode), where a snapshot owns
// the whole DB. The shared branch uses DropPrefix on the group prefix instead.
func (f *FSM) dropAllKeys() error {
	return f.db.Update(func(txn MetadataTxn) error {
		it := txn.NewIterator(MetaIteratorOptions{PrefetchValues: true})
		var keysToDelete [][]byte
		for it.Rewind(); it.Valid(); it.Next() {
			keysToDelete = append(keysToDelete, it.Item().KeyCopy(nil))
		}
		it.Close()
		for _, k := range keysToDelete {
			if err := txn.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}

// Restore replaces this group's metadata state from a snapshot, refusing any
// snapshot whose store-meta record does not carry FormatVersion 2 (satisfies
// raft.Snapshotter; SnapshotManager.Restore supplies the store-meta record
// loaded alongside the payload). Snapshot blob keys must be GROUP-RELATIVE; the
// group prefix is applied on write. Validation runs BEFORE any mutation so a
// rejected restore leaves existing state intact.
func (f *FSM) Restore(meta raft.SnapshotMeta, data []byte) error {
	if meta.FormatVersion != raft.FSMSnapshotFormatVersion {
		return fmt.Errorf("FSM.Restore: unsupported snapshot FormatVersion %d (want %d)", meta.FormatVersion, raft.FSMSnapshotFormatVersion)
	}
	state, err := unmarshalSnapshotState(data)
	if err != nil {
		return fmt.Errorf("FSM.Restore: decode snapshot: %w", err)
	}
	// Keys in the blob must be group-RELATIVE. Reject any that already carry this
	// group's prefix (a mis-encode). Only meaningful when we actually have a prefix.
	if f.keys.isShared() {
		for k := range state {
			if f.keys.HasPrefix([]byte(k)) {
				return fmt.Errorf("FSM.Restore: snapshot key %q already carries a group prefix — refusing", k)
			}
		}
	}
	// Drop only this group's keys; for the empty keyspace fall back to the legacy
	// whole-DB delete loop (the snapshot owns the entire DB in that mode).
	if f.keys.isShared() {
		if err := f.db.DropPrefix(f.keys.Prefix(nil)); err != nil {
			return fmt.Errorf("FSM.Restore: DropPrefix: %w", err)
		}
	} else {
		if err := f.dropAllKeys(); err != nil {
			return fmt.Errorf("FSM.Restore: drop existing keys: %w", err)
		}
	}
	if restoreCrashAfterDrop != nil {
		restoreCrashAfterDrop()
	}
	// Re-encode keys with the group prefix on write.
	return f.db.Update(func(txn MetadataTxn) error {
		for k, v := range state {
			if err := txn.Set(f.keys.Key([]byte(k)), v); err != nil {
				return err
			}
		}
		return nil
	})
}

// applySetRing is a no-op stub; ring-based placement has been removed.
// The switch case and this function will be deleted in Task 7 along with ring.go.
func (f *FSM) applySetRing(_ MetadataTxn, _ []byte) error { return nil }
