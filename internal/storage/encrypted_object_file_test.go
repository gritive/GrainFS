package storage

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
)

func testEncryptor(t *testing.T) *encrypt.Encryptor {
	t.Helper()
	key := bytes.Repeat([]byte{0x44}, 32)
	enc, err := encrypt.NewEncryptor(key)
	require.NoError(t, err)
	return enc
}

func TestEncryptedObjectFileRoundTripAndNoPlaintext(t *testing.T) {
	path := filepath.Join(t.TempDir(), "object")
	enc := testEncryptor(t)
	plaintext := bytes.Repeat([]byte("sensitive-local-object-"), 4096)

	size, etag, err := writeEncryptedObjectFile(path, enc, "local-object:physical-1", bytes.NewReader(plaintext))
	require.NoError(t, err)
	require.Equal(t, int64(len(plaintext)), size)
	require.NotEmpty(t, etag)

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "sensitive-local-object")

	rc, err := openEncryptedObjectFile(path, enc, "local-object:physical-1", size)
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)
}

func TestEncryptedObjectFileReadAtWriteAtAndTruncate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "object")
	enc := testEncryptor(t)
	domain := "local-object:physical-2"

	size, _, err := writeEncryptedObjectFile(path, enc, domain, bytes.NewReader([]byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")))
	require.NoError(t, err)
	require.Equal(t, int64(26), size)

	size, etag, err := writeAtEncryptedObjectFile(path, enc, domain, 5, []byte("-----"), size)
	require.NoError(t, err)
	require.Equal(t, int64(26), size)
	require.NotEmpty(t, etag)

	buf := make([]byte, 10)
	n, err := readAtEncryptedObjectFile(path, enc, domain, size, 0, buf)
	require.NoError(t, err)
	require.Equal(t, 10, n)
	require.Equal(t, "ABCDE-----", string(buf))

	size, err = truncateEncryptedObjectFile(path, enc, domain, size, 8)
	require.NoError(t, err)
	require.Equal(t, int64(8), size)

	rc, err := openEncryptedObjectFile(path, enc, domain, size)
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, "ABCDE---", string(got))
}
