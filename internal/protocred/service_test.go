package protocred

import (
	"strings"
	"testing"
	"time"
)

func TestServiceCreateGetListAndHints(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	svc := NewService(NewStore(), WithNow(func() time.Time { return now }))

	secret, err := svc.Create(CreateRequest{
		SAID: "node-a", Protocol: ProtocolNBD, Resource: "volume/devdisk", Mode: ModeRW,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if secret.ID == "" || secret.Secret == "" {
		t.Fatalf("secret missing fields: %+v", secret)
	}
	if got := secret.ConnectionHint["export_name"]; !strings.HasPrefix(got, "devdisk@") {
		t.Fatalf("export hint = %q, want devdisk@...", got)
	}

	item, err := svc.Get(secret.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if item.SecretHint == "" || strings.Contains(item.SecretHint, secret.Secret) {
		t.Fatalf("unsafe secret hint: item=%+v secret=%q", item, secret.Secret)
	}
	if item.SAID != "node-a" || item.Protocol != ProtocolNBD || item.Resource != "volume/devdisk" || item.Mode != ModeRW {
		t.Fatalf("item mismatch: %+v", item)
	}

	items := svc.List(ListFilter{SAID: "node-a", Protocol: ProtocolNBD})
	if len(items) != 1 || items[0].ID != secret.ID {
		t.Fatalf("List = %+v, want one item %s", items, secret.ID)
	}
}

func TestServiceRotateAndRevoke(t *testing.T) {
	svc := NewService(NewStore())
	first, err := svc.Create(CreateRequest{SAID: "node-a", Protocol: ProtocolNBD, Resource: "volume/devdisk", Mode: ModeRO})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	second, err := svc.Rotate(first.ID)
	if err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	if second.ID != first.ID {
		t.Fatalf("Rotate ID = %q, want same credential ID %q", second.ID, first.ID)
	}
	if second.Secret == first.Secret {
		t.Fatal("Rotate reused secret")
	}
	if err := svc.Revoke(first.ID); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	item, err := svc.Get(first.ID)
	if err != nil {
		t.Fatalf("Get revoked: %v", err)
	}
	if item.RevokedAt == nil {
		t.Fatalf("RevokedAt not set: %+v", item)
	}
	if _, err := svc.Rotate(first.ID); err == nil {
		t.Fatal("Rotate revoked credential succeeded")
	}
}

func TestServiceValidationAndExpiry(t *testing.T) {
	now := time.Unix(200, 0).UTC()
	svc := NewService(NewStore(), WithNow(func() time.Time { return now }))

	if _, err := svc.Create(CreateRequest{SAID: "", Protocol: ProtocolNBD, Resource: "volume/devdisk", Mode: ModeRW}); err == nil {
		t.Fatal("Create without SAID succeeded")
	}
	if _, err := svc.Create(CreateRequest{SAID: "node-a", Protocol: "ftp", Resource: "volume/devdisk", Mode: ModeRW}); err == nil {
		t.Fatal("Create with invalid protocol succeeded")
	}
	if _, err := svc.Create(CreateRequest{SAID: "node-a", Protocol: ProtocolNBD, Resource: "bad", Mode: ModeRW}); err == nil {
		t.Fatal("Create with invalid resource succeeded")
	}
	past := now.Add(-time.Second)
	secret, err := svc.Create(CreateRequest{SAID: "node-a", Protocol: ProtocolNBD, Resource: "volume/devdisk", Mode: ModeRW, ExpiresAt: &past})
	if err != nil {
		t.Fatalf("Create expired: %v", err)
	}
	item, err := svc.Get(secret.ID)
	if err != nil {
		t.Fatalf("Get expired metadata should work: %v", err)
	}
	if item.ExpiresAt == nil {
		t.Fatalf("ExpiresAt not stored: %+v", item)
	}
}
