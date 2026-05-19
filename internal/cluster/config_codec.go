package cluster

import (
	"fmt"
	"sort"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// encodeMetaConfigSnapshot serializes a map[string]string into a
// MetaConfigSnapshot FlatBuffers buffer used as the GCFG trailer payload.
//
// Keys are sorted ascending so the serialized bytes are deterministic — two
// nodes with identical config state must produce byte-identical snapshots
// (raft snapshot replication compares hashes). Mirrors the dek_codec pattern.
func encodeMetaConfigSnapshot(entries map[string]string) ([]byte, error) {
	b := clusterBuilderPool.Get()

	// Build ConfigEntry offsets in reverse order (FlatBuffers convention).
	keys := make([]string, 0, len(entries))
	for k := range entries {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	entryOffs := make([]flatbuffers.UOffsetT, len(keys))
	for i := len(keys) - 1; i >= 0; i-- {
		k := keys[i]
		kOff := b.CreateString(k)
		vOff := b.CreateString(entries[k])
		clusterpb.ConfigEntryStart(b)
		clusterpb.ConfigEntryAddKey(b, kOff)
		clusterpb.ConfigEntryAddValue(b, vOff)
		entryOffs[i] = clusterpb.ConfigEntryEnd(b)
	}

	clusterpb.MetaConfigSnapshotStartEntriesVector(b, len(entryOffs))
	for i := len(entryOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(entryOffs[i])
	}
	entriesVec := b.EndVector(len(entryOffs))

	clusterpb.MetaConfigSnapshotStart(b)
	clusterpb.MetaConfigSnapshotAddEntries(b, entriesVec)
	return fbFinish(b, clusterpb.MetaConfigSnapshotEnd(b)), nil
}

// decodeMetaConfigSnapshot parses a MetaConfigSnapshot FlatBuffers buffer
// and returns the key→value map.
func decodeMetaConfigSnapshot(data []byte) (map[string]string, error) {
	snap, err := fbSafe(data, func(d []byte) *clusterpb.MetaConfigSnapshot {
		return clusterpb.GetRootAsMetaConfigSnapshot(d, 0)
	})
	if err != nil {
		return nil, fmt.Errorf("config_codec: MetaConfigSnapshot: %w", err)
	}

	out := make(map[string]string, snap.EntriesLength())
	var entry clusterpb.ConfigEntry
	for i := 0; i < snap.EntriesLength(); i++ {
		if snap.Entries(&entry, i) {
			out[string(entry.Key())] = string(entry.Value())
		}
	}
	return out, nil
}

// EncodeConfigPutPayload serializes a ConfigPut inner payload (the DataBytes
// portion of a MetaCmd envelope). Exposed for use by packages that need to
// construct a ConfigPut payload outside the cluster package (e.g. tests,
// dispatchers).
func EncodeConfigPutPayload(key, value string) ([]byte, error) {
	return encodeMetaConfigPutCmd(key, value)
}

// DecodeConfigPutPayload parses a ConfigPut inner payload and returns key and
// value. Exposed for dispatchers that receive raw hook payloads.
func DecodeConfigPutPayload(data []byte) (key, value string, err error) {
	return decodeMetaConfigPutCmd(data)
}

// encodeMetaConfigPutCmd serializes a ConfigPut payload (inner data bytes of
// a MetaCmd envelope — wrap with encodeMetaCmd to get the full envelope).
func encodeMetaConfigPutCmd(key, value string) ([]byte, error) {
	b := clusterBuilderPool.Get()
	keyOff := b.CreateString(key)
	valOff := b.CreateString(value)
	clusterpb.MetaConfigPutCmdStart(b)
	clusterpb.MetaConfigPutCmdAddKey(b, keyOff)
	clusterpb.MetaConfigPutCmdAddValue(b, valOff)
	return fbFinish(b, clusterpb.MetaConfigPutCmdEnd(b)), nil
}

// decodeMetaConfigPutCmd parses the inner ConfigPut payload bytes.
func decodeMetaConfigPutCmd(data []byte) (key, value string, err error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaConfigPutCmd {
		return clusterpb.GetRootAsMetaConfigPutCmd(d, 0)
	})
	if err != nil {
		return "", "", fmt.Errorf("config_codec: ConfigPut: %w", err)
	}
	return string(t.Key()), string(t.Value()), nil
}

// encodeMetaConfigDeleteCmd serializes a ConfigDelete payload.
func encodeMetaConfigDeleteCmd(key string) ([]byte, error) {
	b := clusterBuilderPool.Get()
	keyOff := b.CreateString(key)
	clusterpb.MetaConfigDeleteCmdStart(b)
	clusterpb.MetaConfigDeleteCmdAddKey(b, keyOff)
	return fbFinish(b, clusterpb.MetaConfigDeleteCmdEnd(b)), nil
}

// decodeMetaConfigDeleteCmd parses the inner ConfigDelete payload bytes.
func decodeMetaConfigDeleteCmd(data []byte) (key string, err error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaConfigDeleteCmd {
		return clusterpb.GetRootAsMetaConfigDeleteCmd(d, 0)
	})
	if err != nil {
		return "", fmt.Errorf("config_codec: ConfigDelete: %w", err)
	}
	return string(t.Key()), nil
}
