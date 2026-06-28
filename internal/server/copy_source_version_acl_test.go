package server

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/internal/server/servertest"
	"github.com/gritive/GrainFS/internal/storage"
)

// aclParitySpy records whether HeadObjectVersion was called and controls the
// ACL returned per-path: latest (HeadObject) = public-read, specific version
// (HeadObjectVersion) = private. This exposes the loadCopySourceObject dispatch
// bug: pre-fix the function calls HeadObject (public-read → ACL passes → copy
// falls through to 501 NotImplemented); post-fix it calls HeadObjectVersion
// (private → ACL gate → 403 Forbidden).
type aclParitySpy struct {
	storage.Backend
	headObjectVersionCalled bool
}

func (s *aclParitySpy) GetBucketVersioning(_ string) (string, error) {
	return "Suspended", nil
}

func (s *aclParitySpy) SetBucketVersioning(_, _ string) error {
	return nil
}

// HeadObject simulates the latest version having a public-read ACL.
func (s *aclParitySpy) HeadObject(_ context.Context, _, key string) (*storage.Object, error) {
	return &storage.Object{Key: key, ACL: uint8(s3auth.ACLPublicRead)}, nil
}

// HeadObjectVersion simulates the requested specific version having a private ACL.
func (s *aclParitySpy) HeadObjectVersion(_ context.Context, _, key, versionID string) (*storage.Object, error) {
	s.headObjectVersionCalled = true
	return &storage.Object{Key: key, VersionID: versionID, ACL: uint8(s3auth.ACLPrivate)}, nil
}

// TestHandleCopyObject_VersionedSource_OldPrivateVersion_Denied verifies that
// CopyObject with ?versionId= uses the ACL of the requested version, not the
// latest. Pre-fix: HeadObject (public-read) is used → ACL check passes → falls
// through to 501 (no GetObjectVersion adapter). Post-fix: HeadObjectVersion
// (private) is used → 403 AccessDenied.
func TestHandleCopyObject_VersionedSource_OldPrivateVersion_Denied(t *testing.T) {
	real, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { real.Close() })
	require.NoError(t, real.CreateBucket(context.Background(), "src-bkt"))
	require.NoError(t, real.CreateBucket(context.Background(), "dst-bkt"))

	spy := &aclParitySpy{Backend: real}
	port := servertest.FreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	startTestServer(t, addr, spy)
	base := "http://" + addr

	// Anonymous CopyObject: src-bkt/src-key?versionId=V1 (private) → dst-bkt/dst-key.
	// Latest version would be public-read (passes pre-fix), but V1 is private.
	req, err := http.NewRequest(http.MethodPut, base+"/dst-bkt/dst-key", strings.NewReader(""))
	require.NoError(t, err)
	req.Header.Set("x-amz-copy-source", "/src-bkt/src-key?versionId=V1")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	assert.Equal(t, http.StatusForbidden, resp.StatusCode,
		"anonymous copy of a private versioned source must be denied (403 not 501); body=%s", string(body))
}

// TestUploadPartCopy_VersionedSource_OldPrivateVersion_Denied proves the shared
// loadCopySourceObject helper (called by both CopyObject and UploadPartCopy)
// dispatches to HeadObjectVersion when VersionID is set. Anonymous MPU init
// requires auth, so the full UploadPartCopy HTTP flow is not exercised here;
// the unit assertion on the shared dispatch helper covers both call sites.
func TestUploadPartCopy_VersionedSource_OldPrivateVersion_Denied(t *testing.T) {
	real, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, real.CreateBucket(context.Background(), "src-bkt"))

	spy := &aclParitySpy{Backend: real}
	srv := New("127.0.0.1:0", spy)

	obj, err := srv.loadCopySourceObject(context.Background(), storage.ObjectRef{
		Bucket:    "src-bkt",
		Key:       "src-key",
		VersionID: "V1",
	})
	require.NoError(t, err)
	assert.True(t, spy.headObjectVersionCalled,
		"loadCopySourceObject must call HeadObjectVersion when VersionID is set")
	assert.Equal(t, uint8(s3auth.ACLPrivate), obj.ACL,
		"ACL from HeadObjectVersion (private) must be returned for the post-load ACL gate")
}
