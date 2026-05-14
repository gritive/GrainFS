package audit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/icebergcatalog"
	"github.com/gritive/GrainFS/internal/storage"
)

// Bootstrap ensures the grainfs-audit bucket, audit namespace, and audit.s3 Iceberg
// table exist. It is idempotent: already-exists errors are silently ignored.
// Call once at startup before starting the Committer.
func Bootstrap(ctx context.Context, catalog icebergcatalog.Catalog, backend auditBackend) error {
	if err := backend.CreateBucket(ctx, BucketName); err != nil {
		if !errors.Is(err, storage.ErrBucketAlreadyExists) {
			return fmt.Errorf("audit bootstrap: create bucket %q: %w", BucketName, err)
		}
		log.Debug().Str("bucket", BucketName).Msg("audit bootstrap: bucket already exists")
	}

	if err := catalog.CreateNamespace(ctx, []string{Namespace}, nil); err != nil && !errors.Is(err, icebergcatalog.ErrNamespaceExists) {
		return fmt.Errorf("audit bootstrap: create namespace %q: %w", Namespace, err)
	}

	ident := icebergcatalog.Identifier{Namespace: []string{Namespace}, Name: TableS3}
	if tbl, err := catalog.LoadTable(ctx, ident); err == nil {
		migrated, changed, err := MigrateMetadataToCurrent(tbl.Metadata, time.Now().UnixMilli())
		if err != nil {
			return fmt.Errorf("audit bootstrap: migrate metadata: %w", err)
		}
		if changed {
			metaKey := fmt.Sprintf("metadata/s3/%05d-%s.metadata.json", getInt64FromMetadata(migrated, "last-sequence-number"), uuid.New().String())
			metaLocation := fmt.Sprintf("s3://%s/%s", BucketName, metaKey)
			if _, err := backend.PutObject(ctx, BucketName, metaKey, bytes.NewReader(migrated), "application/json"); err != nil {
				return fmt.Errorf("audit bootstrap: write migrated metadata.json: %w", err)
			}
			if _, err := catalog.CommitTable(ctx, ident, icebergcatalog.CommitTableInput{
				ExpectedMetadataLocation: tbl.MetadataLocation,
				NewMetadataLocation:      metaLocation,
				Metadata:                 migrated,
			}); err != nil {
				if errors.Is(err, icebergcatalog.ErrCommitFailed) {
					latest, loadErr := catalog.LoadTable(ctx, ident)
					if loadErr == nil {
						_, stillChanged, migrateErr := MigrateMetadataToCurrent(latest.Metadata, time.Now().UnixMilli())
						if migrateErr != nil {
							return fmt.Errorf("audit bootstrap: recheck migrated metadata: %w", migrateErr)
						}
						if !stillChanged {
							log.Info().Str("table", Namespace+"."+TableS3).Msg("audit bootstrap: metadata migrated by another node")
							return nil
						}
					}
				}
				return fmt.Errorf("audit bootstrap: commit migrated metadata: %w", err)
			}
			log.Info().Str("table", Namespace+"."+TableS3).Msg("audit bootstrap: metadata migrated")
		}
		log.Debug().Str("table", Namespace+"."+TableS3).Msg("audit bootstrap: table exists, skipping")
		return nil
	} else if !errors.Is(err, icebergcatalog.ErrTableNotFound) {
		return fmt.Errorf("audit bootstrap: load table: %w", err)
	}

	tableUUID := uuid.New().String()
	s3Location := fmt.Sprintf("s3://%s/", BucketName)
	nowMs := time.Now().UnixMilli()
	metaJSON := []byte(fmt.Sprintf(S3InitialMetadata, tableUUID, s3Location, nowMs))

	metaKey := fmt.Sprintf("metadata/s3/00000-%s.metadata.json", uuid.New().String())
	metaLocation := fmt.Sprintf("s3://%s/%s", BucketName, metaKey)

	if _, err := backend.PutObject(ctx, BucketName, metaKey, bytes.NewReader(metaJSON), "application/json"); err != nil {
		return fmt.Errorf("audit bootstrap: write initial metadata.json: %w", err)
	}

	if _, err := catalog.CreateTable(ctx, ident, icebergcatalog.CreateTableInput{
		MetadataLocation: metaLocation,
		Metadata:         metaJSON,
	}); err != nil && !errors.Is(err, icebergcatalog.ErrTableExists) {
		return fmt.Errorf("audit bootstrap: create table: %w", err)
	}

	log.Info().Str("table", Namespace+"."+TableS3).Str("location", s3Location).Msg("audit bootstrap: table created")
	return nil
}

func getInt64FromMetadata(raw []byte, key string) int64 {
	var meta map[string]any
	if err := json.Unmarshal(raw, &meta); err != nil {
		return 0
	}
	return getInt64(meta, key)
}
