package e2e

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// Volume REST API tests

type volumeResp struct {
	Name      string `json:"name"`
	Size      int64  `json:"size"`
	BlockSize int    `json:"block_size"`
}

func createVolumeEventually(t *testing.T, name string, size int64) volumeResp {
	t.Helper()
	url := fmt.Sprintf("%s/volumes/%s?size=%d", testServerURL, name, size)
	return createVolumeRequestEventually(t, name, func() (*http.Request, error) {
		return http.NewRequest(http.MethodPut, url, nil)
	})
}

func createVolumeJSONEventually(t *testing.T, name string, body string) volumeResp {
	t.Helper()
	url := fmt.Sprintf("%s/volumes/%s", testServerURL, name)
	return createVolumeRequestEventually(t, name, func() (*http.Request, error) {
		req, err := http.NewRequest(http.MethodPut, url, strings.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		return req, nil
	})
}

func createVolumeRequestEventually(t *testing.T, name string, newReq func() (*http.Request, error)) volumeResp {
	t.Helper()
	var lastErr error
	var lastStatus int
	var lastBody []byte
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		req, err := newReq()
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			lastErr = err
			time.Sleep(500 * time.Millisecond)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		lastStatus = resp.StatusCode
		lastBody = body
		if resp.StatusCode != http.StatusCreated {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		var vol volumeResp
		if err := json.Unmarshal(body, &vol); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		return vol
	}
	t.Fatalf("create volume %s: lastErr=%v status=%d body=%s", name, lastErr, lastStatus, lastBody)
	return volumeResp{}
}

func TestVolume_CreateAndGet(t *testing.T) {
	vol := createVolumeEventually(t, "test-vol-1", 1048576)
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
	expected := []string{"list-vol-a", "list-vol-b"}
	for _, name := range expected {
		createVolumeEventually(t, name, 4096)
	}

	resp, err := http.Get(testServerURL + "/volumes/")
	if err != nil {
		t.Fatalf("list volumes: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, body)
	}

	var vols []volumeResp
	if err := json.NewDecoder(resp.Body).Decode(&vols); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	found := make(map[string]bool, len(vols))
	for _, vol := range vols {
		found[vol.Name] = true
	}
	for _, name := range expected {
		if !found[name] {
			t.Fatalf("expected volume %s in list, got %+v", name, vols)
		}
	}
}

func TestVolume_Delete(t *testing.T) {
	createVolumeEventually(t, "del-vol", 4096)

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
	vol := createVolumeJSONEventually(t, "json-vol", `{"size": 8192}`)
	if vol.Name != "json-vol" {
		t.Fatalf("expected name json-vol, got %s", vol.Name)
	}
	if vol.Size != 8192 {
		t.Fatalf("expected size 8192, got %d", vol.Size)
	}
}
