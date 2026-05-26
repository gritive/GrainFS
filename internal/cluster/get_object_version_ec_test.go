package cluster

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetObjectVersion_ReconstructsEC(t *testing.T) {
	backend := NewSingletonBackendForTest(t)

	const selfAddr = "addr-self:9001"
	shardDir := t.TempDir()
	svc := NewShardService(shardDir, nil, withTestWAL(t))
	backend.SetShardService(svc, []string{selfAddr})
	backend.SetECConfig(ECConfig{DataShards: 1, ParityShards: 1}) // 1+1 so single node works

	require.NoError(t, backend.CreateBucket(context.Background(), "bucket"))
	content := []byte("ec versioned round-trip")
	obj, err := backend.PutObject(context.Background(), "bucket", "key", bytes.NewReader(content), "application/octet-stream")
	require.NoError(t, err)
	require.NotEmpty(t, obj.VersionID, "EC PutObject must assign a versionID")

	rc, got, err := backend.GetObjectVersion("bucket", "key", obj.VersionID)
	require.NoError(t, err, "GetObjectVersion must reconstruct the EC-stored version, not fail on a missing plain file")
	defer rc.Close()
	body, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, content, body, "content must round-trip via the EC reconstruct path")
	assert.Equal(t, obj.VersionID, got.VersionID)
}
