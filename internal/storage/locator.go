package storage

import (
	"path/filepath"
	"strings"
)

// LocatorScheme classifies how a SegmentRef.BlobID addresses its chunk.
type LocatorScheme uint8

const (
	// LocatorLegacy is a key-scoped segment (objects/<bucket>/<key>_segments/<blobID>)
	// or a packblob-materialized chunk. Bare UUIDv7 BlobIDs (no scheme prefix) are
	// implicit legacy — this is the backward-compatible default.
	LocatorLegacy LocatorScheme = iota
	// LocatorCAS is a content-addressed canonical chunk in the (bucket,key)-independent
	// CAS namespace. Ref is the plaintext BLAKE3 content-hash. Physical storage of CAS
	// chunks lands in a later plan; until then read-path rejects it.
	LocatorCAS
)

const (
	casLocatorPrefix    = "cas://"
	legacyLocatorPrefix = "legacy://"
)

// Locator is the parsed view of a SegmentRef.BlobID. read-path uses it to route
// physical access and (later) decryption AAD domain by scheme.
type Locator struct {
	Scheme LocatorScheme
	// Ref is the scheme-specific identifier: CAS content-hash for LocatorCAS,
	// the bare blobID for LocatorLegacy.
	Ref string
}

// ParseLocator interprets a SegmentRef.BlobID string. A bare value with no
// recognized scheme prefix is implicit legacy (backward compatible). An explicit
// "legacy://<bucket>/<key>/<blobID>" is parsed down to its trailing blobID.
func ParseLocator(blobID string) Locator {
	switch {
	case strings.HasPrefix(blobID, casLocatorPrefix):
		return Locator{Scheme: LocatorCAS, Ref: blobID[len(casLocatorPrefix):]}
	case strings.HasPrefix(blobID, legacyLocatorPrefix):
		rest := blobID[len(legacyLocatorPrefix):]
		if i := strings.LastIndexByte(rest, '/'); i >= 0 {
			rest = rest[i+1:]
		}
		return Locator{Scheme: LocatorLegacy, Ref: rest}
	default:
		return Locator{Scheme: LocatorLegacy, Ref: blobID}
	}
}

// SegmentKnownPath is the canonical orphan-sweep path for a raw segment blob:
// "<bucket>/<key>_segments/<normalized-blobID>". The scrubber's known-set and
// every frozen-path source MUST build paths through this one function so they
// can never drift (a drift drops a referenced chunk from the known-set, and the
// scrubber then deletes a still-referenced chunk -> data loss).
//
// The key is canonicalized identically to the on-disk segment layout
// (filepath.Join cleans trailing slashes, "//", "./", "../"), so a referenced
// segment whose object key is non-canonical (e.g. "a/b/") still matches the
// orphan walker's disk-derived path and is never wrongly swept (data loss).
func SegmentKnownPath(bucket, key, blobID string) string {
	// Mirror filepath.Join(bucketDir, key) relative to bucketDir: clean the key
	// and drop any leading separator so it lands directly under the bucket dir.
	cleanKey := filepath.ToSlash(filepath.Clean(filepath.FromSlash(key)))
	cleanKey = strings.TrimPrefix(cleanKey, "/")
	return bucket + "/" + cleanKey + "_segments/" + ParseLocator(blobID).Ref
}

// String renders the locator. LocatorCAS gets the "cas://" prefix; LocatorLegacy
// renders its bare Ref verbatim (no prefix) to preserve existing on-disk BlobIDs.
func (l Locator) String() string {
	if l.Scheme == LocatorCAS {
		return casLocatorPrefix + l.Ref
	}
	return l.Ref
}
