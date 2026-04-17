package nfsserver

import (
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNFSServerStartsAndAcceptsConnections(t *testing.T) {
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)

	srv := NewServer(backend, "nfs-test-vol", nil)

	// Use ephemeral port
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	go func() {
		_ = srv.ListenAndServe(fmt.Sprintf("127.0.0.1:%d", port))
	}()

	// Wait for server to be ready
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 500*time.Millisecond)
		if err == nil {
			conn.Close()
			// Server is accepting connections
			t.Cleanup(func() { srv.Close() })
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatal("NFS server did not start accepting connections in time")
}

func TestAddrBeforeListen(t *testing.T) {
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)

	srv := NewServer(backend, "test-vol", nil)

	// Before listening, Addr should return nil
	assert.Nil(t, srv.Addr())
}

func TestAddrAfterListen(t *testing.T) {
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)

	srv := NewServer(backend, "test-vol", nil)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	go func() {
		_ = srv.ListenAndServe(fmt.Sprintf("127.0.0.1:%d", port))
	}()

	// Wait for server to be ready
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 500*time.Millisecond)
		if err == nil {
			conn.Close()
			addr := srv.Addr()
			assert.NotNil(t, addr)
			assert.Contains(t, addr.String(), fmt.Sprintf("%d", port))
			t.Cleanup(func() { srv.Close() })
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("NFS server did not start accepting connections in time")
}

func TestCloseBeforeListen(t *testing.T) {
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)

	srv := NewServer(backend, "test-vol", nil)

	// Close before listen should return nil (no listener to close)
	err = srv.Close()
	assert.NoError(t, err)
}

// failBackend is a mock that fails HeadBucket, causing vfs.New to fail.
type failBackend struct{}

func (f *failBackend) CreateBucket(string) error                            { return nil }
func (f *failBackend) HeadBucket(string) error                              { return fmt.Errorf("mock: bucket not found") }
func (f *failBackend) DeleteBucket(string) error                            { return fmt.Errorf("not implemented") }
func (f *failBackend) ListBuckets() ([]string, error)                       { return nil, fmt.Errorf("not implemented") }
func (f *failBackend) PutObject(string, string, io.Reader, string) (*storage.Object, error) {
	return nil, fmt.Errorf("not implemented")
}
func (f *failBackend) GetObject(string, string) (io.ReadCloser, *storage.Object, error) {
	return nil, nil, fmt.Errorf("not implemented")
}
func (f *failBackend) HeadObject(string, string) (*storage.Object, error) {
	return nil, fmt.Errorf("not implemented")
}
func (f *failBackend) DeleteObject(string, string) error { return fmt.Errorf("not implemented") }
func (f *failBackend) ListObjects(string, string, int) ([]*storage.Object, error) {
	return nil, fmt.Errorf("not implemented")
}
func (f *failBackend) CreateMultipartUpload(string, string, string) (*storage.MultipartUpload, error) {
	return nil, fmt.Errorf("not implemented")
}
func (f *failBackend) UploadPart(string, string, string, int, io.Reader) (*storage.Part, error) {
	return nil, fmt.Errorf("not implemented")
}
func (f *failBackend) CompleteMultipartUpload(string, string, string, []storage.Part) (*storage.Object, error) {
	return nil, fmt.Errorf("not implemented")
}
func (f *failBackend) AbortMultipartUpload(string, string, string) error {
	return fmt.Errorf("not implemented")
}

func TestListenAndServeVFSError(t *testing.T) {
	srv := NewServer(&failBackend{}, "fail-vol", nil)

	err := srv.ListenAndServe("127.0.0.1:0")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "create vfs")
}

func TestListenAndServeInvalidAddr(t *testing.T) {
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)

	srv := NewServer(backend, "test-vol", nil)

	// Use an invalid address to trigger net.Listen error
	err = srv.ListenAndServe("invalid-address-no-port")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "nfs listen")
}
