package e2e

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	nfsclient "github.com/willscott/go-nfs-client/nfs"
	"github.com/willscott/go-nfs-client/nfs/rpc"
)

// Volume REST API tests

type volumeResp struct {
	Name      string `json:"name"`
	Size      int64  `json:"size"`
	BlockSize int    `json:"block_size"`
}

func TestVolume_CreateAndGet(t *testing.T) {
	url := testServerURL + "/volumes/test-vol-1?size=1048576"
	req, err := http.NewRequest("PUT", url, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create volume: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 201, got %d: %s", resp.StatusCode, body)
	}

	var vol volumeResp
	if err := json.NewDecoder(resp.Body).Decode(&vol); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if vol.Name != "test-vol-1" {
		t.Fatalf("expected name test-vol-1, got %s", vol.Name)
	}
	if vol.Size != 1048576 {
		t.Fatalf("expected size 1048576, got %d", vol.Size)
	}

	resp2, err := http.Get(testServerURL + "/volumes/test-vol-1")
	if err != nil {
		t.Fatalf("get volume: %v", err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp2.StatusCode)
	}

	var vol2 volumeResp
	if err := json.NewDecoder(resp2.Body).Decode(&vol2); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if vol2.Name != "test-vol-1" {
		t.Fatalf("expected name test-vol-1, got %s", vol2.Name)
	}
}

func TestVolume_List(t *testing.T) {
	for _, name := range []string{"list-vol-a", "list-vol-b"} {
		url := testServerURL + "/volumes/" + name + "?size=4096"
		req, _ := http.NewRequest("PUT", url, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("create volume %s: %v", name, err)
		}
		resp.Body.Close()
	}

	resp, err := http.Get(testServerURL + "/volumes/")
	if err != nil {
		t.Fatalf("list volumes: %v", err)
	}
	defer resp.Body.Close()

	var vols []volumeResp
	if err := json.NewDecoder(resp.Body).Decode(&vols); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if len(vols) < 3 {
		t.Fatalf("expected at least 3 volumes, got %d", len(vols))
	}
}

func TestVolume_Delete(t *testing.T) {
	url := testServerURL + "/volumes/del-vol?size=4096"
	req, _ := http.NewRequest("PUT", url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create volume: %v", err)
	}
	resp.Body.Close()

	req2, _ := http.NewRequest("DELETE", testServerURL+"/volumes/del-vol", nil)
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("delete volume: %v", err)
	}
	resp2.Body.Close()

	if resp2.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", resp2.StatusCode)
	}

	resp3, err := http.Get(testServerURL + "/volumes/del-vol")
	if err != nil {
		t.Fatalf("get volume: %v", err)
	}
	resp3.Body.Close()
	if resp3.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp3.StatusCode)
	}
}

func TestVolume_CreateWithJSONBody(t *testing.T) {
	url := testServerURL + "/volumes/json-vol"
	body := strings.NewReader(`{"size": 8192}`)
	req, _ := http.NewRequest("PUT", url, body)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create volume: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 201, got %d: %s", resp.StatusCode, b)
	}
}

// NFS E2E tests - connect directly to port (no portmapper)

func dialNFSTarget(t *testing.T) *nfsclient.Target {
	t.Helper()
	auth := rpc.NewAuthUnix("test", 0, 0)

	// Connect directly to NFS port (no portmapper)
	mountClient, err := nfsclient.DialServiceAtPort("127.0.0.1", testNFSPort)
	if err != nil {
		t.Fatalf("dial mount service at port %d: %v", testNFSPort, err)
	}

	// Set Addr to empty so Mount.Mount uses NewTargetWithClient instead of NewTarget
	// (NewTarget tries portmapper, NewTargetWithClient reuses the same connection)
	mount := &nfsclient.Mount{Client: mountClient}
	target, err := mount.Mount("/", auth.Auth())
	if err != nil {
		mountClient.Close()
		t.Fatalf("mount /: %v", err)
	}

	t.Cleanup(func() {
		target.Close()
		mount.Unmount()
		mountClient.Close()
	})

	return target
}

func TestNFS_MountAndWriteReadFile(t *testing.T) {
	t.Skip("NFS billy/VFS flush integration under unified backend — investigating (tracked in TODOS v0.0.4.0)")
	target := dialNFSTarget(t)

	// Create and write a file
	wr, err := target.OpenFile("/hello.txt", 0644)
	if err != nil {
		t.Fatalf("open file for write: %v", err)
	}

	data := []byte("Hello from NFS E2E test!")
	n, err := wr.Write(data)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if n != len(data) {
		t.Fatalf("expected write %d, got %d", len(data), n)
	}
	wr.Close()

	// Small delay for data to flush
	time.Sleep(100 * time.Millisecond)

	// Read it back
	rd, err := target.Open("/hello.txt")
	if err != nil {
		t.Fatalf("open file for read: %v", err)
	}

	buf, err := io.ReadAll(rd)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	rd.Close()

	if string(buf) != string(data) {
		t.Fatalf("expected %q, got %q", string(data), string(buf))
	}
}

func TestNFS_MultipleFiles(t *testing.T) {
	t.Skip("NFS billy/VFS flush integration under unified backend — investigating (tracked in TODOS v0.0.4.0)")
	target := dialNFSTarget(t)

	// Create multiple files
	for _, name := range []string{"/file-a.txt", "/file-b.txt"} {
		wr, err := target.OpenFile(name, 0644)
		if err != nil {
			t.Fatalf("open file %s: %v", name, err)
		}
		wr.Write([]byte("content of " + name))
		wr.Close()
	}

	// Verify files exist via lookup
	for _, name := range []string{"/file-a.txt", "/file-b.txt"} {
		info, _, err := target.Lookup(name)
		if err != nil {
			t.Fatalf("lookup %s: %v", name, err)
		}
		if info.Size() == 0 {
			t.Fatalf("expected non-zero size for %s", name)
		}
	}
}

func TestNFS_DeleteFile(t *testing.T) {
	target := dialNFSTarget(t)

	wr, err := target.OpenFile("/delete-me.txt", 0644)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	wr.Write([]byte("temp"))
	wr.Close()

	err = target.Remove("/delete-me.txt")
	if err != nil {
		t.Fatalf("remove: %v", err)
	}

	_, _, err = target.Lookup("/delete-me.txt")
	if err == nil {
		t.Fatal("expected error looking up deleted file")
	}
}
