package p9server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"

	"github.com/gritive/GrainFS/internal/storage"
)

const p9MetaPrefix = "__meta/"

type p9FileMeta struct {
	Mode   uint32 `json:"mode"`
	Mtime  int64  `json:"mtime_ns"`
	Target string `json:"target,omitempty"`
}

func p9MetaSidecarKey(key string) string {
	return p9MetaPrefix + key
}

func isP9ReservedName(name string) bool {
	return name == "__meta"
}

func isP9ReservedKey(key string) bool {
	return key == "__meta" || strings.HasPrefix(key, p9MetaPrefix)
}

func loadP9FileMeta(ctx context.Context, backend storage.Backend, bucket, key string) p9FileMeta {
	rc, _, err := backend.GetObject(ctx, bucket, p9MetaSidecarKey(key))
	if err != nil {
		return p9FileMeta{Mode: 0644}
	}
	if rc == nil {
		return p9FileMeta{Mode: 0644}
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return p9FileMeta{Mode: 0644}
	}
	var meta p9FileMeta
	if err := json.Unmarshal(data, &meta); err != nil || meta.Mode == 0 {
		meta.Mode = 0644
	}
	return meta
}

func saveP9FileMeta(ctx context.Context, backend storage.Backend, bucket, key string, meta p9FileMeta) error {
	if meta.Mode == 0 {
		meta.Mode = 0644
	}
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	_, err = backend.PutObject(ctx, bucket, p9MetaSidecarKey(key), bytes.NewReader(data), "application/json")
	return err
}
