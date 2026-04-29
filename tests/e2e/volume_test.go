package e2e

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
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
