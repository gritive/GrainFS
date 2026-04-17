package packblob

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompression_Concurrent_RoundTrip(t *testing.T) {
	const goroutines = 100
	data := bytes.Repeat([]byte("compress me "), 500)

	var wg sync.WaitGroup
	errs := make(chan error, goroutines)

	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			c, err := compress(data)
			if err != nil {
				errs <- err
				return
			}
			got, err := decompress(c)
			if err != nil {
				errs <- err
				return
			}
			if !bytes.Equal(got, data) {
				errs <- fmt.Errorf("data mismatch after compress/decompress")
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

func TestBlobStore_Compression_RoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	bs, err := NewBlobStore(tmpDir, 64*1024*1024)
	require.NoError(t, err)
	defer bs.Close()

	bs.EnableCompression()

	data := bytes.Repeat([]byte("hello world "), 1000)
	loc, err := bs.Append("key1", data)
	require.NoError(t, err)

	got, err := bs.Read(loc)
	require.NoError(t, err)
	require.Equal(t, data, got)
}

func TestBlobStore_Compression_StoredSizeSmaller(t *testing.T) {
	tmpDir := t.TempDir()
	bs, err := NewBlobStore(tmpDir, 64*1024*1024)
	require.NoError(t, err)
	defer bs.Close()

	bs.EnableCompression()

	data := bytes.Repeat([]byte("hello world "), 1000)
	loc, err := bs.Append("key1", data)
	require.NoError(t, err)

	// Compressed length stored in BlobLocation.Length should be smaller
	require.Less(t, int(loc.Length), len(data), "stored (compressed) size should be smaller than original")
}

func TestBlobStore_CompressionDisabled_Passthrough(t *testing.T) {
	tmpDir := t.TempDir()
	bs, err := NewBlobStore(tmpDir, 64*1024*1024)
	require.NoError(t, err)
	defer bs.Close()

	data := bytes.Repeat([]byte("hello world "), 100)
	loc, err := bs.Append("key1", data)
	require.NoError(t, err)

	got, err := bs.Read(loc)
	require.NoError(t, err)
	require.Equal(t, data, got)
}
