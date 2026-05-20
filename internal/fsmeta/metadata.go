package fsmeta

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/gritive/GrainFS/internal/storage"
)

type Kind string

const (
	KindRegular Kind = ""
	KindSymlink Kind = "symlink"
	MetaPrefix       = "__meta/"
)

type FileMeta struct {
	Mode   uint32 `json:"mode"`
	Mtime  int64  `json:"mtime_ns"`
	Kind   Kind   `json:"kind,omitempty"`
	Target string `json:"target,omitempty"`
}

func SidecarKey(key string) string {
	return MetaPrefix + key
}

func IsReservedName(name string) bool {
	return name == strings.TrimSuffix(MetaPrefix, "/")
}

func IsReservedKey(key string) bool {
	return key == strings.TrimSuffix(MetaPrefix, "/") || strings.HasPrefix(key, MetaPrefix)
}

func Load(ctx context.Context, backend storage.Backend, bucket, key string) FileMeta {
	meta, err := LoadStrict(ctx, backend, bucket, key)
	if err != nil || meta.Mode == 0 {
		return FileMeta{Mode: 0644}
	}
	return meta
}

func LoadStrict(ctx context.Context, backend storage.Backend, bucket, key string) (FileMeta, error) {
	rc, _, err := backend.GetObject(ctx, bucket, SidecarKey(key))
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotFound) {
			return FileMeta{Mode: 0644}, nil
		}
		return FileMeta{Mode: 0644}, err
	}
	if rc == nil {
		return FileMeta{Mode: 0644}, nil
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return FileMeta{Mode: 0644}, err
	}

	var meta FileMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return FileMeta{Mode: 0644}, err
	}
	if meta.Mode == 0 {
		meta.Mode = 0644
	}
	if err := meta.Validate(); err != nil {
		return FileMeta{Mode: 0644}, err
	}
	return meta, nil
}

func Save(ctx context.Context, backend storage.Backend, bucket, key string, meta FileMeta) error {
	if meta.Mode == 0 {
		meta.Mode = 0644
	}
	if err := meta.Validate(); err != nil {
		return err
	}

	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	_, err = backend.PutObject(ctx, bucket, SidecarKey(key), bytes.NewReader(data), "application/json")
	return err
}

func SaveSymlink(ctx context.Context, backend storage.Backend, bucket, key, target string, mode uint32, mtime int64) error {
	return Save(ctx, backend, bucket, key, FileMeta{
		Mode:   mode,
		Mtime:  mtime,
		Kind:   KindSymlink,
		Target: target,
	})
}

func (m FileMeta) IsSymlink() bool {
	return m.Kind == KindSymlink
}

func (m FileMeta) Validate() error {
	switch m.Kind {
	case KindRegular:
	case KindSymlink:
		if m.Target == "" {
			return fmt.Errorf("symlink metadata target is empty")
		}
	default:
		return fmt.Errorf("unsupported metadata kind %q", m.Kind)
	}
	return nil
}
