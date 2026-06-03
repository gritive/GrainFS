package cluster

import (
	"context"
	"strings"

	"github.com/dgraph-io/badger/v4"
	"github.com/gritive/GrainFS/internal/storage"
)

func (b *DistributedBackend) ListObjects(ctx context.Context, bucket, prefix string, maxKeys int) ([]*storage.Object, error) {
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return nil, err
	}

	var objects []*storage.Object
	err := b.db.View(func(txn *badger.Txn) error {
		// Load latest-version pointers for this bucket so we can dedupe versioned
		// entries down to a single row per base key (skipping delete markers).
		latMap := make(map[string]string) // base key → latest versionID
		rawLatPrefix := []byte("lat:" + bucket + "/")
		latPrefix := b.ks().Prefix(rawLatPrefix)
		itLat := txn.NewIterator(badger.DefaultIteratorOptions)
		for itLat.Seek(latPrefix); itLat.ValidForPrefix(latPrefix); itLat.Next() {
			rawKey := b.ks().MustStrip(itLat.Item().Key())
			baseKey := string(rawKey[len(rawLatPrefix):])
			_ = itLat.Item().Value(func(v []byte) error {
				latMap[baseKey] = string(v)
				return nil
			})
		}
		itLat.Close()

		// Prefixed scan on obj:{bucket}/{prefix}. For base keys that appear in
		// latMap we emit exactly the version that's current, skipping all other
		// versioned entries. For keys not in latMap (legacy non-versioned data)
		// we emit the single entry we find.
		emitted := make(map[string]bool)
		rawBucketPfx := []byte("obj:" + bucket + "/")
		pfx := b.ks().Prefix([]byte("obj:" + bucket + "/" + prefix))
		bucketPfx := b.ks().Prefix(rawBucketPfx)
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		count := 0
		for it.Seek(pfx); it.ValidForPrefix(bucketPfx); it.Next() {
			if !it.ValidForPrefix(pfx) {
				break
			}
			if count >= maxKeys {
				break
			}
			rawKey := b.ks().MustStrip(it.Item().Key())
			rest := string(rawKey[len(rawBucketPfx):])

			// Derive the base key. A versioned key is "{baseKey}/{versionID}";
			// a legacy key is just "{baseKey}" with no trailing segment.
			baseKey := rest
			isVersioned := false
			if slash := strings.LastIndex(rest, "/"); slash >= 0 {
				candidateBase := rest[:slash]
				candidateVID := rest[slash+1:]
				if lat, ok := latMap[candidateBase]; ok && lat == candidateVID {
					baseKey = candidateBase
					isVersioned = true
				} else if _, baseInLat := latMap[candidateBase]; baseInLat {
					// This is a non-latest version of a versioned key — skip.
					continue
				}
			}
			// A versioned entry "foo/{versionID}" deduces to baseKey="foo", but if
			// the caller asked for prefix "foo/", we must NOT return "foo" — that
			// would cause isDir("foo") to return true for a regular file.
			if !strings.HasPrefix(baseKey, prefix) {
				continue
			}
			if emitted[baseKey] {
				continue
			}

			// If the base key has a lat: pointer but this iteration hit the
			// legacy unversioned `obj:{bucket}/{baseKey}` entry first, we
			// should wait and emit the versioned one. Skip this legacy entry.
			if !isVersioned {
				if _, inLat := latMap[baseKey]; inLat {
					continue
				}
			}

			var obj storage.Object
			val, err := b.itemValueCopy(it.Item())
			if err != nil {
				return err
			}
			m, err := unmarshalObjectMeta(val)
			if err != nil {
				return err
			}
			if m.ETag != deleteMarkerETag {
				obj = storage.Object{
					Key:          m.Key,
					Size:         m.Size,
					ContentType:  m.ContentType,
					ETag:         m.ETag,
					LastModified: m.LastModified,
					ACL:          m.ACL,
					UserMetadata: cloneStringMap(m.UserMetadata),
					SSEAlgorithm: m.SSEAlgorithm,
					Parts:        m.Parts,
					// Tags copied (not aliased) — m's backing bytes are reused
					// by badger once the View tx returns.
					Tags: append([]storage.Tag(nil), m.Tags...),
				}
				if isVersioned {
					obj.VersionID = latMap[baseKey]
				}
			}
			if err != nil {
				return err
			}
			if obj.Key == "" {
				// Skipped (tombstone or empty meta).
				continue
			}
			objects = append(objects, &obj)
			emitted[baseKey] = true
			count++
		}
		return nil
	})
	return objects, err
}

// ListObjectsPage returns one S3 ListObjects page from this DistributedBackend's
// local meta store, honoring marker. truncated is true when the iterator
// stopped because maxKeys was reached and at least one more matching base
// key would have followed. Used by GroupBackend on forwarded reads and by
// ClusterCoordinator's fallback path when bucket has no FSM object-index
// source; without this the fallback silently truncated pages whose marker
// fell past the first maxKeys+1 prefix-matching keys.
func (b *DistributedBackend) ListObjectsPage(ctx context.Context, bucket, prefix, marker string, maxKeys int) ([]*storage.Object, bool, error) {
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return nil, false, err
	}
	var (
		objects   []*storage.Object
		truncated bool
	)
	err := b.db.View(func(txn *badger.Txn) error {
		latMap := make(map[string]string)
		rawLatPrefix := []byte("lat:" + bucket + "/")
		latPrefix := b.ks().Prefix(rawLatPrefix)
		itLat := txn.NewIterator(badger.DefaultIteratorOptions)
		for itLat.Seek(latPrefix); itLat.ValidForPrefix(latPrefix); itLat.Next() {
			rawKey := b.ks().MustStrip(itLat.Item().Key())
			baseKey := string(rawKey[len(rawLatPrefix):])
			_ = itLat.Item().Value(func(v []byte) error {
				latMap[baseKey] = string(v)
				return nil
			})
		}
		itLat.Close()

		emitted := make(map[string]bool)
		rawBucketPfx := []byte("obj:" + bucket + "/")
		pfx := b.ks().Prefix([]byte("obj:" + bucket + "/" + prefix))
		bucketPfx := b.ks().Prefix(rawBucketPfx)
		seek := pfx
		if marker != "" {
			// Resume strictly after `marker`. Append NUL so we land on the
			// first key whose suffix sorts past marker — versions of marker
			// itself ("marker/<vid>") also sort past "marker\x00", so they
			// remain reachable and the dedupe logic below filters them
			// against `emitted` and `latMap`.
			seek = b.ks().Prefix(append([]byte("obj:"+bucket+"/"+marker), 0))
		}
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(seek); it.ValidForPrefix(bucketPfx); it.Next() {
			if !it.ValidForPrefix(pfx) {
				break
			}
			rawKey := b.ks().MustStrip(it.Item().Key())
			rest := string(rawKey[len(rawBucketPfx):])

			baseKey := rest
			isVersioned := false
			if slash := strings.LastIndex(rest, "/"); slash >= 0 {
				candidateBase := rest[:slash]
				candidateVID := rest[slash+1:]
				if lat, ok := latMap[candidateBase]; ok && lat == candidateVID {
					baseKey = candidateBase
					isVersioned = true
				} else if _, baseInLat := latMap[candidateBase]; baseInLat {
					continue
				}
			}
			if !strings.HasPrefix(baseKey, prefix) {
				continue
			}
			if marker != "" && baseKey <= marker {
				continue
			}
			if emitted[baseKey] {
				continue
			}
			if !isVersioned {
				if _, inLat := latMap[baseKey]; inLat {
					continue
				}
			}
			if len(objects) >= maxKeys {
				truncated = true
				break
			}

			var obj storage.Object
			val, err := b.itemValueCopy(it.Item())
			if err != nil {
				return err
			}
			m, err := unmarshalObjectMeta(val)
			if err != nil {
				return err
			}
			if m.ETag == deleteMarkerETag {
				continue
			}
			obj = storage.Object{
				Key:          m.Key,
				Size:         m.Size,
				ContentType:  m.ContentType,
				ETag:         m.ETag,
				LastModified: m.LastModified,
				ACL:          m.ACL,
				UserMetadata: cloneStringMap(m.UserMetadata),
				SSEAlgorithm: m.SSEAlgorithm,
				Parts:        m.Parts,
				// Tags copied (not aliased) — m's backing bytes are reused by
				// badger once the View tx returns.
				Tags: append([]storage.Tag(nil), m.Tags...),
			}
			if isVersioned {
				obj.VersionID = latMap[baseKey]
			}
			if obj.Key == "" {
				continue
			}
			objects = append(objects, &obj)
			emitted[baseKey] = true
		}
		return nil
	})
	return objects, truncated, err
}

func (b *DistributedBackend) WalkObjects(ctx context.Context, bucket, prefix string, fn func(*storage.Object) error) error {
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return err
	}
	return b.db.View(func(txn *badger.Txn) error {
		latMap := make(map[string]string)
		rawLatPrefix := []byte("lat:" + bucket + "/")
		latPrefix := b.ks().Prefix(rawLatPrefix)
		itLat := txn.NewIterator(badger.DefaultIteratorOptions)
		for itLat.Seek(latPrefix); itLat.ValidForPrefix(latPrefix); itLat.Next() {
			rawKey := b.ks().MustStrip(itLat.Item().Key())
			baseKey := string(rawKey[len(rawLatPrefix):])
			_ = itLat.Item().Value(func(v []byte) error {
				latMap[baseKey] = string(v)
				return nil
			})
		}
		itLat.Close()

		emitted := make(map[string]bool)
		rawBucketPfx := []byte("obj:" + bucket + "/")
		pfx := b.ks().Prefix([]byte("obj:" + bucket + "/" + prefix))
		bucketPfx := b.ks().Prefix(rawBucketPfx)
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(pfx); it.ValidForPrefix(bucketPfx); it.Next() {
			if !it.ValidForPrefix(pfx) {
				break
			}
			rawKey := b.ks().MustStrip(it.Item().Key())
			rest := string(rawKey[len(rawBucketPfx):])

			baseKey := rest
			isVersioned := false
			if slash := strings.LastIndex(rest, "/"); slash >= 0 {
				candidateBase := rest[:slash]
				candidateVID := rest[slash+1:]
				if lat, ok := latMap[candidateBase]; ok && lat == candidateVID {
					baseKey = candidateBase
					isVersioned = true
				} else if _, baseInLat := latMap[candidateBase]; baseInLat {
					continue
				}
			}
			if !strings.HasPrefix(baseKey, prefix) {
				continue
			}
			if emitted[baseKey] {
				continue
			}
			if !isVersioned {
				if _, inLat := latMap[baseKey]; inLat {
					continue
				}
			}

			var obj storage.Object
			val, err := b.itemValueCopy(it.Item())
			if err != nil {
				return err
			}
			m, err := unmarshalObjectMeta(val)
			if err != nil {
				return err
			}
			if m.ETag != deleteMarkerETag {
				obj = storage.Object{
					Key:          m.Key,
					Size:         m.Size,
					ContentType:  m.ContentType,
					ETag:         m.ETag,
					LastModified: m.LastModified,
					ACL:          m.ACL,
					UserMetadata: cloneStringMap(m.UserMetadata),
					SSEAlgorithm: m.SSEAlgorithm,
					Parts:        m.Parts,
					// Tags copied (not aliased) — m's backing bytes are reused
					// by badger once the View tx returns.
					Tags: append([]storage.Tag(nil), m.Tags...),
				}
				if isVersioned {
					obj.VersionID = latMap[baseKey]
				}
			}
			if obj.Key == "" {
				continue
			}
			emitted[baseKey] = true
			if err := fn(&obj); err != nil {
				return err
			}
		}
		return nil
	})
}

// --- Multipart operations ---
