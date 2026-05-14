package audit

import (
	"bytes"
	"context"
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
	if _, err := catalog.LoadTable(ctx, ident); err == nil {
		// table exists — nothing to do
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
