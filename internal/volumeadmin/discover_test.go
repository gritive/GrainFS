package volumeadmin

import (
	"net"
	"os"
	"path/filepath"
	"testing"
)

func TestAutoDiscoverSocket_DataFlagHappyPath(t *testing.T) {
	// macOS unix-socket paths are capped at 104 chars; t.TempDir() under
	// /var/folders/... overflows. Use /tmp directly with manual cleanup.
	dir, err := os.MkdirTemp("/tmp", "grainfs-discover-")
	if err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "admin.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	defer l.Close()

	got, err := AutoDiscoverSocket(dir)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	want := "unix:" + sock
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}
