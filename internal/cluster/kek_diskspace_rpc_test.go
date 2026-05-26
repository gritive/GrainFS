package cluster

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/gritive/GrainFS/internal/transport"
)

func TestKEKDiskSpaceHandler_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	h := NewKEKDiskSpaceHandler("node-A", dir, func(d string) (uint64, error) {
		if d != dir {
			return 0, errors.New("unexpected dir")
		}
		return 1 << 30, nil
	})
	req := &transport.Message{
		Type:    transport.StreamKEKDiskSpaceProbe,
		Payload: encodeKEKDiskSpaceReq(KEKDiskSpaceReq{}),
	}
	resp := h.Handle(req)
	if resp.Status != transport.StatusOK {
		t.Fatalf("handler returned status=%v payload=%q", resp.Status, string(resp.Payload))
	}
	decoded, err := decodeKEKDiskSpaceResp(resp.Payload)
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
	req := &transport.Message{
		Type:    transport.StreamKEKDiskSpaceProbe,
		Payload: encodeKEKDiskSpaceReq(KEKDiskSpaceReq{}),
	}
	resp := h.Handle(req)
	if resp.Status != transport.StatusOK {
		t.Fatalf("handler returned status=%v payload=%q", resp.Status, string(resp.Payload))
	}
	decoded, err := decodeKEKDiskSpaceResp(resp.Payload)
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
	resp := h.Handle(&transport.Message{
		Type:    transport.StreamKEKDiskSpaceProbe,
		Payload: []byte("not the right magic"),
	})
	if resp.Status != transport.StatusError {
		t.Fatalf("expected StatusError on bad magic, got %v", resp.Status)
	}
	if !strings.Contains(string(resp.Payload), "bad request magic") {
		t.Errorf("error payload missing magic marker: %q", string(resp.Payload))
	}
}

func TestKEKDiskSpaceHandler_PropagatesProbeError(t *testing.T) {
	h := NewKEKDiskSpaceHandler("node-err", t.TempDir(), func(string) (uint64, error) {
		return 0, errors.New("synthetic statfs failure")
	})
	resp := h.Handle(&transport.Message{
		Type:    transport.StreamKEKDiskSpaceProbe,
		Payload: encodeKEKDiskSpaceReq(KEKDiskSpaceReq{}),
	})
	if resp.Status != transport.StatusError {
		t.Fatalf("expected StatusError on probe failure, got %v", resp.Status)
	}
	if !strings.Contains(string(resp.Payload), "synthetic statfs failure") {
		t.Errorf("error payload missing underlying cause: %q", string(resp.Payload))
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
