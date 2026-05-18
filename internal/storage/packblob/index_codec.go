package packblob

import (
	"fmt"
	"sort"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/storage/packblob/packblobpb"
)

func encodeIndex(entries map[packedKey]*indexEntry) ([]byte, error) {
	b := flatbuffers.NewBuilder(1024)

	// Stable key order for deterministic output (easier diff in tests).
	keys := make([]packedKey, 0, len(entries))
	for k := range entries {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].bucket != keys[j].bucket {
			return keys[i].bucket < keys[j].bucket
		}
		return keys[i].key < keys[j].key
	})

	entryOffsets := make([]flatbuffers.UOffsetT, 0, len(keys))
	for _, pk := range keys {
		e := entries[pk]
		keyOff := b.CreateString(pk.bucket + "/" + pk.key)

		// BlobLocation table
		packblobpb.BlobLocationStart(b)
		packblobpb.BlobLocationAddBlobId(b, e.Location.BlobID)
		packblobpb.BlobLocationAddOffset(b, e.Location.Offset)
		packblobpb.BlobLocationAddLength(b, e.Location.Length)
		locOff := packblobpb.BlobLocationEnd(b)

		ctOff := b.CreateString(e.ContentType)
		etagOff := b.CreateString(e.ETag)
		sseOff := b.CreateString(e.SSEAlgorithm)

		// user_metadata vector (sorted keys for determinism)
		mdKeys := make([]string, 0, len(e.UserMetadata))
		for k := range e.UserMetadata {
			mdKeys = append(mdKeys, k)
		}
		sort.Strings(mdKeys)
		kvOffsets := make([]flatbuffers.UOffsetT, 0, len(mdKeys))
		for _, k := range mdKeys {
			kOff := b.CreateString(k)
			vOff := b.CreateString(e.UserMetadata[k])
			packblobpb.KVStart(b)
			packblobpb.KVAddKey(b, kOff)
			packblobpb.KVAddValue(b, vOff)
			kvOffsets = append(kvOffsets, packblobpb.KVEnd(b))
		}
		packblobpb.IndexEntryStartUserMetadataVector(b, len(kvOffsets))
		for i := len(kvOffsets) - 1; i >= 0; i-- {
			b.PrependUOffsetT(kvOffsets[i])
		}
		mdVec := b.EndVector(len(kvOffsets))

		packblobpb.IndexEntryStart(b)
		packblobpb.IndexEntryAddKey(b, keyOff)
		packblobpb.IndexEntryAddLocation(b, locOff)
		packblobpb.IndexEntryAddRefcount(b, e.Refcount.Load())
		packblobpb.IndexEntryAddOriginalSize(b, e.OriginalSize)
		packblobpb.IndexEntryAddContentType(b, ctOff)
		packblobpb.IndexEntryAddEtag(b, etagOff)
		packblobpb.IndexEntryAddLastModified(b, e.LastModified)
		packblobpb.IndexEntryAddUserMetadata(b, mdVec)
		packblobpb.IndexEntryAddSseAlgorithm(b, sseOff)
		entryOffsets = append(entryOffsets, packblobpb.IndexEntryEnd(b))
	}

	packblobpb.IndexStartEntriesVector(b, len(entryOffsets))
	for i := len(entryOffsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(entryOffsets[i])
	}
	entriesVec := b.EndVector(len(entryOffsets))

	packblobpb.IndexStart(b)
	packblobpb.IndexAddEntries(b, entriesVec)
	b.Finish(packblobpb.IndexEnd(b))
	return b.FinishedBytes(), nil
}

func decodeIndexStorage(data []byte) (out map[packedKey]*indexEntry, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("packblob: malformed index.bin: %v", r)
		}
	}()
	root := packblobpb.GetRootAsIndex(data, 0)
	n := root.EntriesLength()
	out = make(map[packedKey]*indexEntry, n)
	var raw packblobpb.IndexEntry
	for i := 0; i < n; i++ {
		if !root.Entries(&raw, i) {
			continue
		}
		pk, ok := parsePackedKey(string(raw.Key()))
		if !ok {
			return nil, fmt.Errorf("packblob: corrupt index entry key: %q", string(raw.Key()))
		}
		loc := raw.Location(nil)
		ent := &indexEntry{
			Location: BlobLocation{
				BlobID: loc.BlobId(),
				Offset: loc.Offset(),
				Length: loc.Length(),
			},
			OriginalSize: raw.OriginalSize(),
			ContentType:  string(raw.ContentType()),
			ETag:         string(raw.Etag()),
			LastModified: raw.LastModified(),
			SSEAlgorithm: string(raw.SseAlgorithm()),
		}
		ent.Refcount.Store(raw.Refcount())
		mdN := raw.UserMetadataLength()
		if mdN > 0 {
			md := make(map[string]string, mdN)
			var kv packblobpb.KV
			for j := 0; j < mdN; j++ {
				if !raw.UserMetadata(&kv, j) {
					continue
				}
				md[string(kv.Key())] = string(kv.Value())
			}
			ent.UserMetadata = md
		}
		out[pk] = ent
	}
	return out, nil
}
