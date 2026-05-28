package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/protocred"
	"github.com/gritive/GrainFS/internal/s3auth"
)

type protocolCredentialTestEnvelope struct{}

func (protocolCredentialTestEnvelope) SealProtocolCredentialSecret(_ []byte, plaintext string) ([]byte, error) {
	return []byte(base64.RawStdEncoding.EncodeToString([]byte(plaintext))), nil
}

func (protocolCredentialTestEnvelope) OpenProtocolCredentialSecret(_ []byte, ciphertext []byte) (string, error) {
	plain, err := base64.RawStdEncoding.DecodeString(string(ciphertext))
	if err != nil {
		return "", err
	}
	return string(plain), nil
}

func TestProtocolCredentialAuthS3AllowsMatchingRWCredential(t *testing.T) {
	store := protocred.NewStore()
	envelope := protocolCredentialTestEnvelope{}
	secret, err := protocred.NewService(store, protocred.WithSecretEnvelope(envelope)).Create(protocred.CreateRequest{
		SAID:     "sa-s3",
		Protocol: protocred.ProtocolS3,
		Resource: "bucket/protocred-bucket",
		Mode:     protocred.ModeRW,
	})
	require.NoError(t, err)

	base, backend := setupTestServerWithBackend(t,
		WithAuth([]s3auth.Credentials{{AccessKey: "legacy", SecretKey: "legacy-secret"}}),
		WithProtocolCredentialAuth(store, envelope),
	)
	mustCreateBucket(t, backend, "protocred-bucket")

	req, err := http.NewRequest(http.MethodPut, base+"/protocred-bucket/file.txt", bytes.NewReader([]byte("ok")))
	require.NoError(t, err)
	req.Host = req.URL.Host
	s3auth.SignRequest(req, secret.ID, secret.Secret, "us-east-1")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestProtocolCredentialAuthS3RejectsROCredentialForWrite(t *testing.T) {
	store := protocred.NewStore()
	envelope := protocolCredentialTestEnvelope{}
	secret, err := protocred.NewService(store, protocred.WithSecretEnvelope(envelope)).Create(protocred.CreateRequest{
		SAID:     "sa-s3",
		Protocol: protocred.ProtocolS3,
		Resource: "bucket/protocred-bucket",
		Mode:     protocred.ModeRO,
	})
	require.NoError(t, err)

	base, backend := setupTestServerWithBackend(t,
		WithAuth([]s3auth.Credentials{{AccessKey: "legacy", SecretKey: "legacy-secret"}}),
		WithProtocolCredentialAuth(store, envelope),
	)
	mustCreateBucket(t, backend, "protocred-bucket")

	req, err := http.NewRequest(http.MethodPut, base+"/protocred-bucket/file.txt", bytes.NewReader([]byte("denied")))
	require.NoError(t, err)
	req.Host = req.URL.Host
	s3auth.SignRequest(req, secret.ID, secret.Secret, "us-east-1")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestProtocolCredentialAuthS3RejectsWrongBucket(t *testing.T) {
	store := protocred.NewStore()
	envelope := protocolCredentialTestEnvelope{}
	secret, err := protocred.NewService(store, protocred.WithSecretEnvelope(envelope)).Create(protocred.CreateRequest{
		SAID:     "sa-s3",
		Protocol: protocred.ProtocolS3,
		Resource: "bucket/allowed-bucket",
		Mode:     protocred.ModeRW,
	})
	require.NoError(t, err)

	base, backend := setupTestServerWithBackend(t,
		WithAuth([]s3auth.Credentials{{AccessKey: "legacy", SecretKey: "legacy-secret"}}),
		WithProtocolCredentialAuth(store, envelope),
	)
	mustCreateBucket(t, backend, "other-bucket")

	req, err := http.NewRequest(http.MethodPut, base+"/other-bucket/file.txt", bytes.NewReader([]byte("denied")))
	require.NoError(t, err)
	req.Host = req.URL.Host
	s3auth.SignRequest(req, secret.ID, secret.Secret, "us-east-1")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestProtocolCredentialAuthS3RejectsCrossBucketCopySource(t *testing.T) {
	store := protocred.NewStore()
	envelope := protocolCredentialTestEnvelope{}
	secret, err := protocred.NewService(store, protocred.WithSecretEnvelope(envelope)).Create(protocred.CreateRequest{
		SAID:     "sa-s3",
		Protocol: protocred.ProtocolS3,
		Resource: "bucket/dest-bucket",
		Mode:     protocred.ModeRW,
	})
	require.NoError(t, err)

	base, backend := setupTestServerWithBackend(t,
		WithAuth([]s3auth.Credentials{{AccessKey: "legacy", SecretKey: "legacy-secret"}}),
		WithProtocolCredentialAuth(store, envelope),
	)
	mustCreateBucket(t, backend, "source-bucket")
	mustCreateBucket(t, backend, "dest-bucket")
	_, err = backend.PutObject(context.Background(), "source-bucket", "source.txt", bytes.NewReader([]byte("secret")), "text/plain")
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPut, base+"/dest-bucket/copied.txt", nil)
	require.NoError(t, err)
	req.Host = req.URL.Host
	req.Header.Set("x-amz-copy-source", "/source-bucket/source.txt")
	s3auth.SignRequest(req, secret.ID, secret.Secret, "us-east-1")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestProtocolCredentialAuthS3RejectsStaleStrict(t *testing.T) {
	store := protocred.NewStore()
	envelope := protocolCredentialTestEnvelope{}
	svc := protocred.NewService(store, protocred.WithSecretEnvelope(envelope))
	secret, err := svc.Create(protocred.CreateRequest{
		SAID:     "sa-s3",
		Protocol: protocred.ProtocolS3,
		Resource: "bucket/protocred-bucket",
		Mode:     protocred.ModeRW,
	})
	require.NoError(t, err)
	_, err = store.ApplyMarkStale(secret.ID, time.Now().UTC(), "policy_changed")
	require.NoError(t, err)

	base, backend := setupTestServerWithBackend(t,
		WithAuth([]s3auth.Credentials{{AccessKey: "legacy", SecretKey: "legacy-secret"}}),
		WithProtocolCredentialAuth(store, envelope),
	)
	mustCreateBucket(t, backend, "protocred-bucket")

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPut, base+"/protocred-bucket/file.txt", bytes.NewReader([]byte("denied")))
	require.NoError(t, err)
	req.Host = req.URL.Host
	s3auth.SignRequest(req, secret.ID, secret.Secret, "us-east-1")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestProtocolCredentialAuthIcebergAllowsMatchingCredential(t *testing.T) {
	store := protocred.NewStore()
	envelope := protocolCredentialTestEnvelope{}
	secret, err := protocred.NewService(store, protocred.WithSecretEnvelope(envelope)).Create(protocred.CreateRequest{
		SAID:     "sa-iceberg",
		Protocol: protocred.ProtocolIceberg,
		Resource: "catalog/tenant-a",
		Mode:     protocred.ModeRO,
	})
	require.NoError(t, err)

	base := setupTestServerWithOptions(t,
		WithAuth([]s3auth.Credentials{{AccessKey: "legacy", SecretKey: "legacy-secret"}}),
		WithProtocolCredentialAuth(store, envelope),
	)

	req, err := http.NewRequest(http.MethodGet, base+"/iceberg/v1/config?warehouse=tenant-a", nil)
	require.NoError(t, err)
	req.Host = req.URL.Host
	s3auth.SignRequest(req, secret.ID, secret.Secret, "us-east-1")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var got struct {
		Defaults map[string]string `json:"defaults"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	require.Equal(t, "tenant-a", got.Defaults["warehouse"])
}

func TestProtocolCredentialAuthIcebergUsesCredentialResourceWhenWarehouseOmitted(t *testing.T) {
	store := protocred.NewStore()
	envelope := protocolCredentialTestEnvelope{}
	secret, err := protocred.NewService(store, protocred.WithSecretEnvelope(envelope)).Create(protocred.CreateRequest{
		SAID:     "sa-iceberg",
		Protocol: protocred.ProtocolIceberg,
		Resource: "catalog/tenant-a",
		Mode:     protocred.ModeRO,
	})
	require.NoError(t, err)

	base := setupTestServerWithOptions(t,
		WithAuth([]s3auth.Credentials{{AccessKey: "legacy", SecretKey: "legacy-secret"}}),
		WithProtocolCredentialAuth(store, envelope),
	)

	req, err := http.NewRequest(http.MethodGet, base+"/iceberg/v1/namespaces", nil)
	require.NoError(t, err)
	req.Host = req.URL.Host
	s3auth.SignRequest(req, secret.ID, secret.Secret, "us-east-1")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.NotEqual(t, http.StatusUnauthorized, resp.StatusCode)
	require.NotEqual(t, http.StatusForbidden, resp.StatusCode)
}

func TestProtocolCredentialAuthRejectsProtocolMismatch(t *testing.T) {
	store := protocred.NewStore()
	envelope := protocolCredentialTestEnvelope{}
	secret, err := protocred.NewService(store, protocred.WithSecretEnvelope(envelope)).Create(protocred.CreateRequest{
		SAID:     "sa-s3",
		Protocol: protocred.ProtocolS3,
		Resource: "bucket/protocred-bucket",
		Mode:     protocred.ModeRO,
	})
	require.NoError(t, err)

	base := setupTestServerWithOptions(t,
		WithAuth([]s3auth.Credentials{{AccessKey: "legacy", SecretKey: "legacy-secret"}}),
		WithProtocolCredentialAuth(store, envelope),
	)

	req, err := http.NewRequest(http.MethodGet, base+"/iceberg/v1/config?warehouse=warehouse", nil)
	require.NoError(t, err)
	req.Host = req.URL.Host
	s3auth.SignRequest(req, secret.ID, secret.Secret, "us-east-1")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}
