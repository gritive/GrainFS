package storage

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOperationsPutObjectWithResultReturnsPreviousSummary(t *testing.T) {
	backend := &mutationResultBackend{
		previous: &Object{Key: "k", Size: 12, ETag: "old", VersionID: "v1"},
	}
	ops := NewOperations(backend)

	result, err := ops.PutObjectWithResult(context.Background(), "b", "k", strings.NewReader("new-data"), "text/plain")

	require.NoError(t, err)
	require.Equal(t, "new", result.Object.ETag)
	require.Equal(t, PreviousObject{
		Exists:    true,
		Size:      12,
		ETag:      "old",
		VersionID: "v1",
	}, result.Previous)
	require.Equal(t, []string{"head:b/k", "put:b/k:text/plain:new-data"}, backend.calls)
}

func TestOperationsPutObjectWithResultContinuesWhenPreviousMissing(t *testing.T) {
	backend := &mutationResultBackend{previousErr: ErrObjectNotFound}
	ops := NewOperations(backend)

	result, err := ops.PutObjectWithResult(context.Background(), "b", "k", strings.NewReader("new"), "text/plain")

	require.NoError(t, err)
	require.False(t, result.Previous.Exists)
	require.Equal(t, []string{"head:b/k", "put:b/k:text/plain:new"}, backend.calls)
}

func TestOperationsPutObjectWithResultFailsBeforeMutationWhenPreviousReadFails(t *testing.T) {
	readErr := errors.New("head failed")
	backend := &mutationResultBackend{previousErr: readErr}
	ops := NewOperations(backend)

	_, err := ops.PutObjectWithResult(context.Background(), "b", "k", strings.NewReader("new"), "text/plain")

	require.ErrorIs(t, err, readErr)
	require.Equal(t, []string{"head:b/k"}, backend.calls)
}

func TestOperationsCopyObjectWithResultReportsDestinationPreviousSummary(t *testing.T) {
	backend := &mutationResultBackend{
		source:     &Object{Key: "src", Size: 7, ETag: "src-etag", ContentType: "text/plain", LastModified: 100},
		sourceBody: "payload",
		previous:   &Object{Key: "dst", Size: 3, ETag: "old-dst", VersionID: "v2"},
	}
	ops := NewOperations(backend)

	result, err := ops.CopyObjectWithResult(context.Background(), CopyObjectRequest{
		Source:      ObjectRef{Bucket: "src-bucket", Key: "src"},
		Destination: ObjectRef{Bucket: "dst-bucket", Key: "dst"},
	})

	require.NoError(t, err)
	require.Equal(t, "new", result.Object.ETag)
	require.Equal(t, PreviousObject{
		Exists:    true,
		Size:      3,
		ETag:      "old-dst",
		VersionID: "v2",
	}, result.Previous)
	require.Equal(t, []string{
		"head:src-bucket/src",
		"head:dst-bucket/dst",
		"get:src-bucket/src",
		"put:dst-bucket/dst:text/plain:payload",
	}, backend.calls)
}

func TestOperationsCopyObjectWithResultFailsBeforeWriteWhenDestinationPreviousReadFails(t *testing.T) {
	readErr := errors.New("destination head failed")
	backend := &mutationResultBackend{
		source:      &Object{Key: "src", Size: 7, ETag: "src-etag", ContentType: "text/plain", LastModified: 100},
		sourceBody:  "payload",
		previousErr: readErr,
	}
	ops := NewOperations(backend)

	_, err := ops.CopyObjectWithResult(context.Background(), CopyObjectRequest{
		Source:      ObjectRef{Bucket: "src-bucket", Key: "src"},
		Destination: ObjectRef{Bucket: "dst-bucket", Key: "dst"},
	})

	require.ErrorIs(t, err, readErr)
	require.Equal(t, []string{"head:src-bucket/src", "head:dst-bucket/dst"}, backend.calls)
}

type mutationResultBackend struct {
	basicBackend
	calls       []string
	source      *Object
	sourceBody  string
	previous    *Object
	previousErr error
}

func (b *mutationResultBackend) HeadObject(_ context.Context, bucket, key string) (*Object, error) {
	b.calls = append(b.calls, "head:"+bucket+"/"+key)
	if bucket == "src-bucket" {
		if b.source == nil {
			return nil, ErrObjectNotFound
		}
		return cloneObject(b.source), nil
	}
	if b.previousErr != nil {
		return nil, b.previousErr
	}
	if b.previous == nil {
		return nil, ErrObjectNotFound
	}
	return cloneObject(b.previous), nil
}

func (b *mutationResultBackend) GetObject(_ context.Context, bucket, key string) (io.ReadCloser, *Object, error) {
	b.calls = append(b.calls, "get:"+bucket+"/"+key)
	if b.source == nil {
		return nil, nil, ErrObjectNotFound
	}
	return io.NopCloser(strings.NewReader(b.sourceBody)), cloneObject(b.source), nil
}

func (b *mutationResultBackend) PutObject(_ context.Context, bucket, key string, r io.Reader, contentType string) (*Object, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	b.calls = append(b.calls, "put:"+bucket+"/"+key+":"+contentType+":"+string(data))
	return &Object{Key: key, Size: int64(len(data)), ETag: "new", ContentType: contentType, VersionID: "new-version"}, nil
}
