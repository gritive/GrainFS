package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/icebergcatalog"
	"github.com/gritive/GrainFS/internal/storage"
)

func migrateLegacySingletonIcebergCatalog(ctx context.Context, legacy *icebergcatalog.Store, catalog icebergcatalog.Catalog, backend storage.Backend) error {
	if legacy == nil || catalog == nil || backend == nil {
		return nil
	}
	exported, err := legacy.ExportLegacyRows(ctx)
	if err != nil {
		return fmt.Errorf("export legacy Iceberg catalog rows: %w", err)
	}
	if len(exported.Namespaces) == 0 && len(exported.Tables) == 0 {
		return nil
	}

	for _, ns := range exported.Namespaces {
		existing, err := catalog.LoadNamespace(ctx, ns.Namespace)
		switch {
		case err == nil:
			if !reflect.DeepEqual(existing, ns.Properties) {
				log.Warn().Strs("namespace", ns.Namespace).Msg("legacy Iceberg namespace properties differ from meta-Raft; keeping meta-Raft value")
			}
			continue
		case errors.Is(err, icebergcatalog.ErrNamespaceNotFound):
			if err := catalog.CreateNamespace(ctx, ns.Namespace, ns.Properties); err != nil && !errors.Is(err, icebergcatalog.ErrNamespaceExists) {
				return fmt.Errorf("migrate legacy Iceberg namespace %v: %w", ns.Namespace, err)
			}
		default:
			return fmt.Errorf("load Iceberg namespace %v before migration: %w", ns.Namespace, err)
		}
	}

	for _, tbl := range exported.Tables {
		existing, err := catalog.LoadTable(ctx, tbl.Identifier)
		switch {
		case err == nil:
			if existing.MetadataLocation != tbl.MetadataLocation {
				return fmt.Errorf("conflicting legacy Iceberg table pointer for %v: meta-Raft=%s legacy=%s", tbl.Identifier, existing.MetadataLocation, tbl.MetadataLocation)
			}
			if !reflect.DeepEqual(existing.Properties, tbl.Properties) {
				log.Warn().Strs("namespace", tbl.Identifier.Namespace).Str("table", tbl.Identifier.Name).Msg("legacy Iceberg table properties differ from meta-Raft; keeping meta-Raft value")
			}
			continue
		case errors.Is(err, icebergcatalog.ErrTableNotFound), errors.Is(err, storage.ErrObjectNotFound), errors.Is(err, storage.ErrBucketNotFound), errors.Is(err, storage.ErrNoSuchBucket):
			if err := ensureIcebergMetadataObject(ctx, backend, tbl.MetadataLocation, tbl.Metadata); err != nil {
				return fmt.Errorf("backfill legacy Iceberg metadata for %v: %w", tbl.Identifier, err)
			}
			_, err := catalog.CreateTable(ctx, tbl.Identifier, icebergcatalog.CreateTableInput{
				MetadataLocation: tbl.MetadataLocation,
				Metadata:         tbl.Metadata,
				Properties:       tbl.Properties,
			})
			if err != nil && !errors.Is(err, icebergcatalog.ErrTableExists) {
				return fmt.Errorf("migrate legacy Iceberg table %v: %w", tbl.Identifier, err)
			}
		default:
			return fmt.Errorf("load Iceberg table %v before migration: %w", tbl.Identifier, err)
		}
	}

	return nil
}

func ensureIcebergMetadataObject(ctx context.Context, backend storage.Backend, location string, metadata []byte) error {
	bucket, key, ok := parseIcebergS3LocationForMigration(location)
	if !ok {
		return fmt.Errorf("invalid Iceberg metadata location: %s", location)
	}
	rc, _, err := backend.GetObject(bucket, key)
	if err == nil {
		_, _ = io.Copy(io.Discard, rc)
		return rc.Close()
	}
	if !errors.Is(err, storage.ErrObjectNotFound) && !errors.Is(err, storage.ErrBucketNotFound) && !errors.Is(err, storage.ErrNoSuchBucket) {
		return err
	}
	if len(bytes.TrimSpace(metadata)) == 0 {
		return fmt.Errorf("legacy metadata JSON is empty for %s", location)
	}
	if errors.Is(err, storage.ErrBucketNotFound) || errors.Is(err, storage.ErrNoSuchBucket) {
		if createErr := backend.CreateBucket(bucket); createErr != nil && !errors.Is(createErr, storage.ErrBucketAlreadyExists) {
			return createErr
		}
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	_, err = backend.PutObject(bucket, key, bytes.NewReader(metadata), "application/json")
	return err
}

func parseIcebergS3LocationForMigration(location string) (bucket, key string, ok bool) {
	const prefix = "s3://"
	if !strings.HasPrefix(location, prefix) {
		return "", "", false
	}
	rest := strings.TrimPrefix(location, prefix)
	slash := strings.IndexByte(rest, '/')
	if slash <= 0 || slash == len(rest)-1 {
		return "", "", false
	}
	return rest[:slash], rest[slash+1:], true
}
