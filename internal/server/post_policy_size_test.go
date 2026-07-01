package server

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/storage"
)

// recordingFormPutBackend captures the PutObjectRequest that the form-upload
// path threads into the backend while delegating real storage to an embedded
// LocalBackend, so the surrounding Operations routing still works.
type recordingFormPutBackend struct {
	storage.Backend
	lastReq *storage.PutObjectRequest
}

func (b *recordingFormPutBackend) PutObjectWithRequest(ctx context.Context, req storage.PutObjectRequest) (*storage.Object, error) {
	cp := req
	b.lastReq = &cp
	return b.Backend.(storage.RequestPutter).PutObjectWithRequest(ctx, req)
}

// TestPutFormObject_ThreadsExactSize proves the S3 POST form-upload path threads
// the multipart FileHeader.Size as an EXACT SizeHint. The cluster backend's
// streaming write requires SizeHintExact (else it errors), so a form upload that
// dropped the size would 500 on a real cluster. The recording backend captures
// the request putFormObject builds.
func TestPutFormObject_ThreadsExactSize(t *testing.T) {
	ctx := context.Background()
	local := cluster.NewSingletonBackendForTest(t)
	require.NoError(t, local.CreateBucket(ctx, "b"))

	rec := &recordingFormPutBackend{Backend: local}
	s := &Server{ops: storage.NewOperations(rec), mutations: NewMutationBroker()}

	const body = "hello form upload body"
	// size is the authoritative multipart FileHeader.Size for this body.
	_, err := s.putFormObject(ctx, "b", "k", strings.NewReader(body), "text/plain", int64(len(body)))
	require.NoError(t, err)

	require.NotNil(t, rec.lastReq, "form upload must go through PutObjectWithRequest (sized path)")
	require.NotNil(t, rec.lastReq.SizeHint, "form upload must thread a SizeHint so the cluster backend streams")
	assert.Equal(t, int64(len(body)), *rec.lastReq.SizeHint, "SizeHint must equal the multipart FileHeader.Size")
	assert.True(t, rec.lastReq.SizeHintExact, "SizeHint must be exact so the cluster streaming write accepts it")
}
