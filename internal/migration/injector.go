// Package migration provides tools for migrating data into GrainFS.
package migration

import (
	"context"
	"errors"
	"io"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/storage"
)

// Source is the minimal interface required from a migration source.
type Source interface {
	ListBuckets() ([]string, error)
	ListObjects(bucket string) ([]string, error)
	GetObject(bucket, key string) (io.ReadCloser, *storage.Object, error)
}

// Stats holds migration results.
type Stats struct {
	Copied  int
	Skipped int
	Errors  int
}

// Destination is the minimal interface required for a migration destination.
type Destination interface {
	CreateBucket(ctx context.Context, bucket string) error
	PutObject(ctx context.Context, bucket, key string, body io.Reader, ct string) (*storage.Object, error)
	GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *storage.Object, error)
}

// Injector copies objects from a source into a destination backend.
type Injector struct {
	src          Source
	dst          Destination
	skipExisting bool
}

// Option configures an Injector.
type Option func(*Injector)

// WithSkipExisting skips objects that already exist in the destination.
func WithSkipExisting(skip bool) Option {
	return func(inj *Injector) { inj.skipExisting = skip }
}

// NewInjector creates a migration injector.
func NewInjector(src Source, dst Destination, opts ...Option) *Injector {
	inj := &Injector{src: src, dst: dst}
	for _, o := range opts {
		o(inj)
	}
	return inj
}

// Run executes the migration and returns aggregate stats.
func (inj *Injector) Run() (Stats, error) {
	var stats Stats

	buckets, err := inj.src.ListBuckets()
	if err != nil {
		return stats, err
	}

	for _, bucket := range buckets {
		if err := inj.dst.CreateBucket(context.Background(), bucket); err != nil {
			if !errors.Is(err, storage.ErrBucketAlreadyExists) {
				log.Warn().Str("bucket", bucket).Err(err).Msg("migration: create bucket failed")
				stats.Errors++
				continue
			}
		}

		keys, err := inj.src.ListObjects(bucket)
		if err != nil {
			log.Warn().Str("bucket", bucket).Err(err).Msg("migration: list objects failed")
			stats.Errors++
			continue
		}

		for _, key := range keys {
			if inj.skipExisting {
				rc, _, err := inj.dst.GetObject(context.Background(), bucket, key)
				if err == nil {
					rc.Close()
					stats.Skipped++
					continue
				}
			}

			rc, obj, err := inj.src.GetObject(bucket, key)
			if err != nil {
				log.Warn().Str("bucket", bucket).Str("key", key).Err(err).Msg("migration: get object failed")
				stats.Errors++
				continue
			}

			ct := ""
			if obj != nil {
				ct = obj.ContentType
			}

			if _, err := inj.dst.PutObject(context.Background(), bucket, key, rc, ct); err != nil {
				rc.Close()
				log.Warn().Str("bucket", bucket).Str("key", key).Err(err).Msg("migration: put object failed")
				stats.Errors++
				continue
			}
			rc.Close()
			stats.Copied++
		}
	}

	return stats, nil
}
