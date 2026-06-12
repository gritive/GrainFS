package cluster

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
)

func TestKEKDiskSpaceHandler_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	h := NewKEKDiskSpaceHandler("node-A", dir, func(d string) (uint64, error) {
		if d != dir {
			return 0, errors.New("unexpected dir")
		}
		return 1 << 30, nil
	})
	resp, herr := h.Handle(encodeKEKDiskSpaceReq(KEKDiskSpaceReq{}))
	if herr != nil {
		t.Fatalf("handler returned error: %v", herr)
	}
	decoded, err := decodeKEKDiskSpaceResp(resp)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if decoded.NodeID != "node-A" {
		t.Errorf("NodeID = %q, want node-A", decoded.NodeID)
	}
	if decoded.KeystorePath != dir {
		t.Errorf("KeystorePath = %q, want %q", decoded.KeystorePath, dir)
	}
	if decoded.FreeBytes != (1 << 30) {
		t.Errorf("FreeBytes = %d, want %d", decoded.FreeBytes, 1<<30)
	}
}

func TestKEKDiskSpaceHandler_DefaultUsesStatfs(t *testing.T) {
	dir := t.TempDir()
	h := NewKEKDiskSpaceHandler("node-default", dir, nil)
	resp, herr := h.Handle(encodeKEKDiskSpaceReq(KEKDiskSpaceReq{}))
	if herr != nil {
		t.Fatalf("handler returned error: %v", herr)
	}
	decoded, err := decodeKEKDiskSpaceResp(resp)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	// Don't assert a magnitude — real filesystems vary. Just confirm syscall
	// succeeded and produced a non-zero report (any tmpdir under load should
	// still have far more than 0 bytes free).
	if decoded.FreeBytes == 0 {
		t.Errorf("FreeBytes = 0, expected real filesystem to report >0")
	}
	if _, err := os.Stat(decoded.KeystorePath); err != nil {
		t.Errorf("KeystorePath %q not stat-able: %v", decoded.KeystorePath, err)
	}
}

func TestKEKDiskSpaceHandler_RejectsBadMagic(t *testing.T) {
	h := NewKEKDiskSpaceHandler("node-X", t.TempDir(), func(string) (uint64, error) {
		return 1, nil
	})
	_, herr := h.Handle([]byte("not the right magic"))
	if herr == nil {
		t.Fatal("expected error on bad magic, got nil")
	}
	if !strings.Contains(herr.Error(), "bad request magic") {
		t.Errorf("error missing magic marker: %q", herr.Error())
	}
}

func TestKEKDiskSpaceHandler_PropagatesProbeError(t *testing.T) {
	h := NewKEKDiskSpaceHandler("node-err", t.TempDir(), func(string) (uint64, error) {
		return 0, errors.New("synthetic statfs failure")
	})
	_, herr := h.Handle(encodeKEKDiskSpaceReq(KEKDiskSpaceReq{}))
	if herr == nil {
		t.Fatal("expected error on probe failure, got nil")
	}
	if !strings.Contains(herr.Error(), "synthetic statfs failure") {
		t.Errorf("error missing underlying cause: %q", herr.Error())
	}
}

func TestGetKEKDiskSpace_RoundTrip(t *testing.T) {
	want := KEKDiskSpaceResp{
		FreeBytes:    42 * (1 << 20),
		KeystorePath: "/var/lib/grainfs/keys",
		NodeID:       "node-B",
	}
	dialer := func(_ context.Context, peer string, payload []byte) ([]byte, error) {
		if peer != "node-B" {
			return nil, errors.New("unexpected peer")
		}
		if _, err := decodeKEKDiskSpaceReq(payload); err != nil {
			return nil, err
		}
		return encodeKEKDiskSpaceResp(want), nil
	}
	got, err := GetKEKDiskSpace(context.Background(), "node-B", dialer)
	if err != nil {
		t.Fatalf("GetKEKDiskSpace: %v", err)
	}
	if got != want {
		t.Errorf("response mismatch: got %+v want %+v", got, want)
	}
}
