package p9server

import (
	"context"

	"github.com/gritive/GrainFS/internal/fsmeta"
	"github.com/gritive/GrainFS/internal/storage"
)

type p9FileMeta = fsmeta.FileMeta

func p9MetaSidecarKey(key string) string {
	return fsmeta.SidecarKey(key)
}

func isP9ReservedName(name string) bool {
	return fsmeta.IsReservedName(name)
}

func isP9ReservedKey(key string) bool {
	return fsmeta.IsReservedKey(key)
}

func loadP9FileMeta(ctx context.Context, backend storage.Backend, bucket, key string) p9FileMeta {
	return fsmeta.Load(ctx, backend, bucket, key)
}

func saveP9FileMeta(ctx context.Context, backend storage.Backend, bucket, key string, meta p9FileMeta) error {
	return fsmeta.Save(ctx, backend, bucket, key, meta)
}
