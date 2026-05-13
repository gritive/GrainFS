package iam

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAdminAPI_CreateSA(t *testing.T) {
	// Empty store → first-SA bootstrap path: ProposeInitFirstSA atomically
	// commits SA + key + wildcard grant. The fake must apply records so the
	// handler's race-detect LookupKey succeeds.
	store := NewStore()
	enc := newTestEncryptor(t)
	p := newFakeProposer()
	p.store = store
	p.enc = enc
	api := NewAdminAPI(store, p, enc)

	body, _ := json.Marshal(SACreateRequest{Name: "alice", Description: "team data"})
	req := httptest.NewRequest("POST", "/admin/iam/sa", bytes.NewReader(body))
	w := httptest.NewRecorder()
	api.HandleSACreate(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", w.Code, w.Body.String())
	}
	var resp SACreateResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Name != "alice" {
		t.Errorf("Name = %q", resp.Name)
	}
	if resp.AccessKey == "" || resp.SecretKey == "" {
		t.Errorf("AccessKey/SecretKey empty")
	}
	if !strings.HasPrefix(resp.AccessKey, "AKGF") {
		t.Errorf("AccessKey prefix = %q, want AKGF*", resp.AccessKey)
	}
	if resp.SAID != DefaultSAID {
		t.Errorf("SAID = %q, want DefaultSAID for first-SA path", resp.SAID)
	}
	if len(resp.Grants) != 1 || resp.Grants[0].Bucket != WildcardBucket || resp.Grants[0].Role != "admin" {
		t.Errorf("Grants = %+v, want [{*,admin}]", resp.Grants)
	}
}

func TestAdminAPI_CreateSA_MissingName(t *testing.T) {
	api := NewAdminAPI(NewStore(), newFakeProposer(), newTestEncryptor(t))
	body, _ := json.Marshal(SACreateRequest{})
	req := httptest.NewRequest("POST", "/admin/iam/sa", bytes.NewReader(body))
	w := httptest.NewRecorder()
	api.HandleSACreate(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestAdminAPI_ListSA(t *testing.T) {
	store := NewStore()
	store.applySACreate(ServiceAccount{ID: "sa-1", Name: "alice"})
	store.applySACreate(ServiceAccount{ID: "sa-2", Name: "bob"})
	store.applyKeyCreate(AccessKey{AccessKey: "AK1", SAID: "sa-1", Status: KeyStatusActive})
	api := NewAdminAPI(store, newFakeProposer(), newTestEncryptor(t))

	req := httptest.NewRequest("GET", "/admin/iam/sa", nil)
	w := httptest.NewRecorder()
	api.HandleSAList(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	var items []SAListItem
	if err := json.Unmarshal(w.Body.Bytes(), &items); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("len = %d, want 2", len(items))
	}
	for _, it := range items {
		if it.SAID == "sa-1" && it.NumKeys != 1 {
			t.Errorf("sa-1 NumKeys = %d, want 1", it.NumKeys)
		}
	}
}

func TestAdminAPI_GetSA(t *testing.T) {
	store := NewStore()
	store.applySACreate(ServiceAccount{ID: "sa-x", Name: "carol"})
	api := NewAdminAPI(store, newFakeProposer(), newTestEncryptor(t))

	req := httptest.NewRequest("GET", "/admin/iam/sa/sa-x", nil)
	w := httptest.NewRecorder()
	api.HandleSAGet(w, req, "sa-x")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	var resp SAGetResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.SAID != "sa-x" || resp.Name != "carol" {
		t.Errorf("got %+v", resp)
	}
}

func TestAdminAPI_GetSA_NotFound(t *testing.T) {
	api := NewAdminAPI(NewStore(), newFakeProposer(), newTestEncryptor(t))
	req := httptest.NewRequest("GET", "/admin/iam/sa/missing", nil)
	w := httptest.NewRecorder()
	api.HandleSAGet(w, req, "missing")
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

func TestAdminAPI_DeleteSA(t *testing.T) {
	store := NewStore()
	store.applySACreate(ServiceAccount{ID: "sa-d", Name: "to-delete"})
	p := newFakeProposer()
	api := NewAdminAPI(store, p, newTestEncryptor(t))

	req := httptest.NewRequest("DELETE", "/admin/iam/sa/sa-d", nil)
	w := httptest.NewRecorder()
	api.HandleSADelete(w, req, "sa-d")

	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d", w.Code)
	}
	// Note: actual deletion happens via FSM apply, which our fakeProposer doesn't run.
	// We only verify the propose call was made.
	if !p.calledSADelete("sa-d") {
		t.Errorf("ProposeSADelete not called")
	}
}

func TestAdminAPI_DeleteSA_NotFound(t *testing.T) {
	api := NewAdminAPI(NewStore(), newFakeProposer(), newTestEncryptor(t))
	req := httptest.NewRequest("DELETE", "/admin/iam/sa/missing", nil)
	w := httptest.NewRecorder()
	api.HandleSADelete(w, req, "missing")
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

func TestAdminAPI_KeyCreate(t *testing.T) {
	store := NewStore()
	store.applySACreate(ServiceAccount{ID: "sa-1", Name: "alice"})
	p := newFakeProposer()
	api := NewAdminAPI(store, p, newTestEncryptor(t))

	req := httptest.NewRequest("POST", "/admin/iam/sa/sa-1/key", strings.NewReader("{}"))
	w := httptest.NewRecorder()
	api.HandleKeyCreate(w, req, "sa-1")

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", w.Code, w.Body.String())
	}
	var resp KeyCreateResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.AccessKey == "" || resp.SecretKey == "" {
		t.Fatal("missing AK/SK")
	}
	if resp.SAID != "sa-1" {
		t.Errorf("SAID = %q", resp.SAID)
	}
	if !p.calledKeyCreate(resp.AccessKey) {
		t.Errorf("ProposeKeyCreate not called for %s", resp.AccessKey)
	}
}

func TestAdminAPI_KeyCreate_PreservesExpiresAt(t *testing.T) {
	store := NewStore()
	store.applySACreate(ServiceAccount{ID: "sa-1", Name: "alice"})
	p := &fakeProposer{store: store}
	api := NewAdminAPI(store, p, newTestEncryptor(t))
	expiresAt := time.Now().UTC().Add(time.Hour).Truncate(time.Nanosecond)
	body, err := json.Marshal(KeyCreateRequest{ExpiresAt: &expiresAt})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	req := httptest.NewRequest("POST", "/admin/iam/sa/sa-1/key", bytes.NewReader(body))
	w := httptest.NewRecorder()
	api.HandleKeyCreate(w, req, "sa-1")

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", w.Code, w.Body.String())
	}
	var resp KeyCreateResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	got, ok := store.LookupKey(resp.AccessKey)
	if !ok {
		t.Fatalf("created key %q was not applied", resp.AccessKey)
	}
	if got.ExpiresAt == nil || !got.ExpiresAt.Equal(expiresAt) {
		t.Fatalf("ExpiresAt = %v, want %v", got.ExpiresAt, expiresAt)
	}
}

func TestAdminAPI_KeyCreate_SAMissing(t *testing.T) {
	api := NewAdminAPI(NewStore(), newFakeProposer(), newTestEncryptor(t))
	req := httptest.NewRequest("POST", "/admin/iam/sa/missing/key", strings.NewReader("{}"))
	w := httptest.NewRecorder()
	api.HandleKeyCreate(w, req, "missing")
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

func TestAdminAPI_KeyRevoke(t *testing.T) {
	store := NewStore()
	store.applySACreate(ServiceAccount{ID: "sa-1"})
	store.applyKeyCreate(AccessKey{AccessKey: "AK-X", SAID: "sa-1", Status: KeyStatusActive})
	p := newFakeProposer()
	api := NewAdminAPI(store, p, newTestEncryptor(t))

	req := httptest.NewRequest("DELETE", "/admin/iam/sa/sa-1/key/AK-X", nil)
	w := httptest.NewRecorder()
	api.HandleKeyRevoke(w, req, "sa-1", "AK-X")

	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d", w.Code)
	}
	if !p.calledKeyRevoke("AK-X") {
		t.Errorf("ProposeKeyRevoke not called")
	}
}

func TestAdminAPI_KeyRevoke_NotFound(t *testing.T) {
	store := NewStore()
	store.applySACreate(ServiceAccount{ID: "sa-1"})
	api := NewAdminAPI(store, newFakeProposer(), newTestEncryptor(t))
	req := httptest.NewRequest("DELETE", "/admin/iam/sa/sa-1/key/AK-MISSING", nil)
	w := httptest.NewRecorder()
	api.HandleKeyRevoke(w, req, "sa-1", "AK-MISSING")
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

func TestAdminAPI_KeyRevoke_WrongSA(t *testing.T) {
	store := NewStore()
	store.applySACreate(ServiceAccount{ID: "sa-1"})
	store.applySACreate(ServiceAccount{ID: "sa-2"})
	store.applyKeyCreate(AccessKey{AccessKey: "AK-X", SAID: "sa-1", Status: KeyStatusActive})
	api := NewAdminAPI(store, newFakeProposer(), newTestEncryptor(t))
	req := httptest.NewRequest("DELETE", "/admin/iam/sa/sa-2/key/AK-X", nil)
	w := httptest.NewRecorder()
	api.HandleKeyRevoke(w, req, "sa-2", "AK-X")
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404 for cross-SA revoke", w.Code)
	}
}

func TestAdminAPI_GrantPut(t *testing.T) {
	store := NewStore()
	store.applySACreate(ServiceAccount{ID: "sa-1"})
	p := newFakeProposer()
	api := NewAdminAPI(store, p, newTestEncryptor(t))

	body, _ := json.Marshal(GrantPutRequest{SAID: "sa-1", Bucket: "bk-1", Role: "Write"})
	req := httptest.NewRequest("PUT", "/admin/iam/grant", bytes.NewReader(body))
	w := httptest.NewRecorder()
	api.HandleGrantPut(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, body=%s", w.Code, w.Body.String())
	}
	if !p.calledGrantPut("sa-1", "bk-1") {
		t.Errorf("ProposeGrantPut not called")
	}
}

func TestAdminAPI_GrantPut_RejectWildcard(t *testing.T) {
	store := NewStore()
	store.applySACreate(ServiceAccount{ID: "sa-1"})
	api := NewAdminAPI(store, newFakeProposer(), newTestEncryptor(t))
	body, _ := json.Marshal(GrantPutRequest{SAID: "sa-1", Bucket: WildcardBucket, Role: "Read"})
	req := httptest.NewRequest("PUT", "/admin/iam/grant", bytes.NewReader(body))
	w := httptest.NewRecorder()
	api.HandleGrantPut(w, req)
	if w.Code != http.StatusForbidden {
		t.Errorf("status = %d, want 403", w.Code)
	}
}

func TestAdminAPI_GrantPut_BadRole(t *testing.T) {
	store := NewStore()
	store.applySACreate(ServiceAccount{ID: "sa-1"})
	api := NewAdminAPI(store, newFakeProposer(), newTestEncryptor(t))
	body, _ := json.Marshal(GrantPutRequest{SAID: "sa-1", Bucket: "b", Role: "Owner"})
	req := httptest.NewRequest("PUT", "/admin/iam/grant", bytes.NewReader(body))
	w := httptest.NewRecorder()
	api.HandleGrantPut(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestAdminAPI_GrantPut_SAMissing(t *testing.T) {
	api := NewAdminAPI(NewStore(), newFakeProposer(), newTestEncryptor(t))
	body, _ := json.Marshal(GrantPutRequest{SAID: "missing", Bucket: "b", Role: "Read"})
	req := httptest.NewRequest("PUT", "/admin/iam/grant", bytes.NewReader(body))
	w := httptest.NewRecorder()
	api.HandleGrantPut(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

func TestAdminAPI_GrantPut_MissingFields(t *testing.T) {
	api := NewAdminAPI(NewStore(), newFakeProposer(), newTestEncryptor(t))
	body, _ := json.Marshal(GrantPutRequest{SAID: "sa-1"})
	req := httptest.NewRequest("PUT", "/admin/iam/grant", bytes.NewReader(body))
	w := httptest.NewRecorder()
	api.HandleGrantPut(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestAdminAPI_GrantDelete(t *testing.T) {
	p := newFakeProposer()
	api := NewAdminAPI(NewStore(), p, newTestEncryptor(t))
	body, _ := json.Marshal(GrantDeleteRequest{SAID: "sa-1", Bucket: "bk-1"})
	req := httptest.NewRequest("DELETE", "/admin/iam/grant", bytes.NewReader(body))
	w := httptest.NewRecorder()
	api.HandleGrantDelete(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d", w.Code)
	}
	if !p.calledGrantDelete("sa-1", "bk-1") {
		t.Errorf("ProposeGrantDelete not called")
	}
}

func TestAdminAPI_GrantDelete_WildcardRoutesToWildcardProposer(t *testing.T) {
	store := NewStore()
	store.applyGrantWildcardPut(Grant{SAID: "sa-1", Role: RoleAdmin})
	store.applyGrantPut(Grant{SAID: "sa-1", Bucket: "bk", Role: RoleRead})
	p := newFakeProposer()
	api := NewAdminAPI(store, p, newTestEncryptor(t))

	body, _ := json.Marshal(GrantDeleteRequest{SAID: "sa-1", Bucket: WildcardBucket})
	req := httptest.NewRequest("DELETE", "/admin/iam/grant", bytes.NewReader(body))
	w := httptest.NewRecorder()
	api.HandleGrantDelete(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, body=%s", w.Code, w.Body.String())
	}
	if !p.calledGrantWildcardDelete("sa-1") {
		t.Errorf("ProposeGrantWildcardDelete not called; calls=%v", p.calls)
	}
	if p.calledGrantDelete("sa-1", WildcardBucket) {
		t.Errorf("wildcard route must not fall through to ProposeGrantDelete")
	}
}

func TestAdminAPI_GrantDelete_WildcardOnDefaultSA_NoExplicitGrants_409(t *testing.T) {
	store := NewStore()
	store.applyGrantWildcardPut(Grant{SAID: DefaultSAID, Role: RoleAdmin})
	p := newFakeProposer()
	api := NewAdminAPI(store, p, newTestEncryptor(t))

	body, _ := json.Marshal(GrantDeleteRequest{SAID: DefaultSAID, Bucket: WildcardBucket})
	req := httptest.NewRequest("DELETE", "/admin/iam/grant", bytes.NewReader(body))
	w := httptest.NewRecorder()
	api.HandleGrantDelete(w, req)

	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409, body=%s", w.Code, w.Body.String())
	}
	if p.calledGrantWildcardDelete(DefaultSAID) {
		t.Errorf("guard must block ProposeGrantWildcardDelete; calls=%v", p.calls)
	}
	if p.calledGrantDelete(DefaultSAID, WildcardBucket) {
		t.Errorf("guard must block any propose; calls=%v", p.calls)
	}
}

func TestAdminAPI_GrantDelete_WildcardOnDefaultSA_WithExplicitGrants_204(t *testing.T) {
	store := NewStore()
	store.applyGrantWildcardPut(Grant{SAID: DefaultSAID, Role: RoleAdmin})
	store.applyGrantPut(Grant{SAID: DefaultSAID, Bucket: "owned", Role: RoleAdmin})
	p := newFakeProposer()
	api := NewAdminAPI(store, p, newTestEncryptor(t))

	body, _ := json.Marshal(GrantDeleteRequest{SAID: DefaultSAID, Bucket: WildcardBucket})
	req := httptest.NewRequest("DELETE", "/admin/iam/grant", bytes.NewReader(body))
	w := httptest.NewRecorder()
	api.HandleGrantDelete(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204, body=%s", w.Code, w.Body.String())
	}
	if !p.calledGrantWildcardDelete(DefaultSAID) {
		t.Errorf("ProposeGrantWildcardDelete not called; calls=%v", p.calls)
	}
}

func TestAdminAPI_GrantDelete_WildcardOnNonDefaultSA_204(t *testing.T) {
	store := NewStore()
	// Non-default SA with wildcard (shouldn't normally happen per P3, but
	// the guard is moot here so removal is allowed unconditionally).
	store.applyGrantWildcardPut(Grant{SAID: "sa-x", Role: RoleAdmin})
	p := newFakeProposer()
	api := NewAdminAPI(store, p, newTestEncryptor(t))

	body, _ := json.Marshal(GrantDeleteRequest{SAID: "sa-x", Bucket: WildcardBucket})
	req := httptest.NewRequest("DELETE", "/admin/iam/grant", bytes.NewReader(body))
	w := httptest.NewRecorder()
	api.HandleGrantDelete(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204, body=%s", w.Code, w.Body.String())
	}
	if !p.calledGrantWildcardDelete("sa-x") {
		t.Errorf("ProposeGrantWildcardDelete not called; calls=%v", p.calls)
	}
}

func TestAdminAPI_GrantList_All(t *testing.T) {
	store := NewStore()
	store.applySACreate(ServiceAccount{ID: "sa-1"})
	store.applyGrantPut(Grant{SAID: "sa-1", Bucket: "b1", Role: RoleRead})
	store.applyGrantPut(Grant{SAID: "sa-1", Bucket: "b2", Role: RoleWrite})
	store.applyGrantWildcardPut(Grant{SAID: "sa-1", Role: RoleAdmin})
	api := NewAdminAPI(store, newFakeProposer(), newTestEncryptor(t))

	req := httptest.NewRequest("GET", "/admin/iam/grant", nil)
	w := httptest.NewRecorder()
	api.HandleGrantList(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	var items []GrantListItem
	_ = json.Unmarshal(w.Body.Bytes(), &items)
	if len(items) != 3 {
		t.Errorf("len = %d, want 3 (b1, b2, *), got %v", len(items), items)
	}
}

func TestAdminAPI_GrantList_FilterBySA(t *testing.T) {
	store := NewStore()
	store.applyGrantPut(Grant{SAID: "sa-1", Bucket: "b1", Role: RoleRead})
	store.applyGrantPut(Grant{SAID: "sa-2", Bucket: "b1", Role: RoleWrite})
	api := NewAdminAPI(store, newFakeProposer(), newTestEncryptor(t))
	req := httptest.NewRequest("GET", "/admin/iam/grant?sa=sa-1", nil)
	w := httptest.NewRecorder()
	api.HandleGrantList(w, req)
	var items []GrantListItem
	_ = json.Unmarshal(w.Body.Bytes(), &items)
	if len(items) != 1 || items[0].SAID != "sa-1" {
		t.Errorf("filter sa=sa-1: got %v", items)
	}
}

func TestAdminAPI_GrantList_FilterByBucket(t *testing.T) {
	store := NewStore()
	store.applyGrantPut(Grant{SAID: "sa-1", Bucket: "b1", Role: RoleRead})
	store.applyGrantPut(Grant{SAID: "sa-1", Bucket: "b2", Role: RoleRead})
	store.applyGrantWildcardPut(Grant{SAID: "sa-1", Role: RoleAdmin})
	api := NewAdminAPI(store, newFakeProposer(), newTestEncryptor(t))
	req := httptest.NewRequest("GET", "/admin/iam/grant?bucket=b1", nil)
	w := httptest.NewRecorder()
	api.HandleGrantList(w, req)
	var items []GrantListItem
	_ = json.Unmarshal(w.Body.Bytes(), &items)
	// bucket filter excludes wildcards (per plan: "bucketFilter == \"\" loop wildcards")
	if len(items) != 1 || items[0].Bucket != "b1" {
		t.Errorf("filter bucket=b1: got %v", items)
	}
}

// TestHandleKeyCreate_Scoped_Happy: SA with grant on "logs", POST {buckets:["logs"]} → 200,
// response echoes scope, ProposeKeyCreateScoped called.
func TestHandleKeyCreate_Scoped_Happy(t *testing.T) {
	store := NewStore()
	store.applySACreate(ServiceAccount{ID: "sa-1", Name: "alice"})
	store.applyGrantPut(Grant{SAID: "sa-1", Bucket: "logs", Role: RoleWrite})
	p := newFakeProposer()
	api := NewAdminAPI(store, p, newTestEncryptor(t))

	body, _ := json.Marshal(map[string]any{"buckets": []string{"logs"}})
	req := httptest.NewRequest("POST", "/admin/iam/sa/sa-1/key", bytes.NewReader(body))
	w := httptest.NewRecorder()
	api.HandleKeyCreate(w, req, "sa-1")

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", w.Code, w.Body.String())
	}
	var resp KeyCreateResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Buckets) != 1 || resp.Buckets[0] != "logs" {
		t.Errorf("Buckets = %v, want [logs]", resp.Buckets)
	}
	// Must have used the scoped propose, not the legacy one.
	found := false
	for _, c := range p.calls {
		if strings.HasPrefix(c, "KeyCreateScoped:") {
			found = true
		}
		if strings.HasPrefix(c, "KeyCreate:") && !strings.HasPrefix(c, "KeyCreateScoped:") {
			t.Errorf("legacy ProposeKeyCreate must not be called for scoped key; calls=%v", p.calls)
		}
	}
	if !found {
		t.Errorf("ProposeKeyCreateScoped not called; calls=%v", p.calls)
	}
}

// TestHandleKeyCreate_OverScope_400: SA only has "logs" grant, POST {buckets:["logs","reports"]} → 400.
func TestHandleKeyCreate_OverScope_400(t *testing.T) {
	store := NewStore()
	store.applySACreate(ServiceAccount{ID: "sa-1", Name: "alice"})
	store.applyGrantPut(Grant{SAID: "sa-1", Bucket: "logs", Role: RoleWrite})
	api := NewAdminAPI(store, newFakeProposer(), newTestEncryptor(t))

	body, _ := json.Marshal(map[string]any{"buckets": []string{"logs", "reports"}})
	req := httptest.NewRequest("POST", "/admin/iam/sa/sa-1/key", bytes.NewReader(body))
	w := httptest.NewRecorder()
	api.HandleKeyCreate(w, req, "sa-1")

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "reports") {
		t.Errorf("body = %q, want mention of 'reports'", w.Body.String())
	}
}

// TestHandleKeyCreate_Sentinel_400: sentinel values ["*"] and ["__system__"] → 400.
func TestHandleKeyCreate_Sentinel_400(t *testing.T) {
	store := NewStore()
	store.applySACreate(ServiceAccount{ID: "sa-1", Name: "alice"})
	api := NewAdminAPI(store, newFakeProposer(), newTestEncryptor(t))

	for _, sentinel := range []string{"*", "__system__"} {
		body, _ := json.Marshal(map[string]any{"buckets": []string{sentinel}})
		req := httptest.NewRequest("POST", "/admin/iam/sa/sa-1/key", bytes.NewReader(body))
		w := httptest.NewRecorder()
		api.HandleKeyCreate(w, req, "sa-1")

		if w.Code != http.StatusBadRequest {
			t.Errorf("sentinel=%q: status = %d, want 400, body=%s", sentinel, w.Code, w.Body.String())
		}
		if !strings.Contains(w.Body.String(), "sentinel") {
			t.Errorf("sentinel=%q: body = %q, want 'sentinel' in error", sentinel, w.Body.String())
		}
	}
}

// TestHandleKeyCreate_EmptyBuckets_LegacyPath: POST {} → 200, legacy ProposeKeyCreate, no scoped call.
func TestHandleKeyCreate_EmptyBuckets_LegacyPath(t *testing.T) {
	store := NewStore()
	store.applySACreate(ServiceAccount{ID: "sa-1", Name: "alice"})
	p := newFakeProposer()
	api := NewAdminAPI(store, p, newTestEncryptor(t))

	req := httptest.NewRequest("POST", "/admin/iam/sa/sa-1/key", strings.NewReader("{}"))
	w := httptest.NewRecorder()
	api.HandleKeyCreate(w, req, "sa-1")

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", w.Code, w.Body.String())
	}
	var resp KeyCreateResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Buckets != nil {
		t.Errorf("Buckets = %v, want nil for legacy path", resp.Buckets)
	}
	// Legacy propose must be called; scoped must NOT.
	legacyCalled := false
	for _, c := range p.calls {
		if c == "KeyCreate:"+resp.AccessKey {
			legacyCalled = true
		}
		if c == "KeyCreateScoped:"+resp.AccessKey {
			t.Errorf("ProposeKeyCreateScoped must not be called for empty-bucket legacy path")
		}
	}
	if !legacyCalled {
		t.Errorf("ProposeKeyCreate not called; calls=%v", p.calls)
	}
}

func TestAdminAPI_BucketUpstream_PutHappyPath(t *testing.T) {
	store := NewStore()
	enc := newTestEncryptor(t)
	fp := newFakeProposer()
	api := NewAdminAPI(store, fp, enc)

	// A9: JSON key is `upstream_url`, NOT `endpoint`.
	body := strings.NewReader(`{
		"bucket":"shared",
		"upstream_url":"http://up.example:9000",
		"access_key":"AKUP",
		"secret_key":"sk-plain"
	}`)
	r := httptest.NewRequest("POST", "/v1/iam/bucket-upstream", body)
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	api.HandleBucketUpstreamPut(w, r)

	if w.Code != http.StatusNoContent {
		t.Fatalf("status: got %d want 204; body=%s", w.Code, w.Body.String())
	}
	if len(fp.bucketUpstreamPuts) != 1 {
		t.Fatalf("propose count: got %d want 1", len(fp.bucketUpstreamPuts))
	}
	got := fp.bucketUpstreamPuts[0]
	if got.Bucket != "shared" || got.Endpoint != "http://up.example:9000" || got.AccessKey != "AKUP" {
		t.Errorf("payload: got %+v", got)
	}
	if got.SecretKey != "sk-plain" {
		t.Errorf("plain secret discarded; got %q", got.SecretKey)
	}
	if len(got.SecretKeyEnc) == 0 {
		t.Error("SecretKeyEnc empty — wrap not performed")
	}
	// A2: AAD = "bucket-upstream:"+bucket. Verify by unwrapping with the prefixed AAD.
	unwrapped, err := UnwrapSecret(enc, "bucket-upstream:shared", got.SecretKeyEnc)
	if err != nil {
		t.Fatalf("unwrap with AAD=bucket-upstream:shared: %v", err)
	}
	if unwrapped != "sk-plain" {
		t.Errorf("unwrap result: got %q want sk-plain", unwrapped)
	}
}

func TestAdminAPI_BucketUpstream_PutValidationErrors(t *testing.T) {
	cases := []struct {
		name string
		body string
		want int
	}{
		// All bodies use the new JSON shape with `upstream_url` per A9.
		{"empty bucket", `{"bucket":"","upstream_url":"http://x","access_key":"AK","secret_key":"S"}`, http.StatusBadRequest},
		{"empty upstream_url", `{"bucket":"valid","upstream_url":"","access_key":"AK","secret_key":"S"}`, http.StatusBadRequest},
		{"bad upstream_url scheme", `{"bucket":"valid","upstream_url":"ftp://x","access_key":"AK","secret_key":"S"}`, http.StatusBadRequest},
		{"empty ak", `{"bucket":"valid","upstream_url":"http://x","access_key":"","secret_key":"S"}`, http.StatusBadRequest},
		{"empty sk", `{"bucket":"valid","upstream_url":"http://x","access_key":"AK","secret_key":""}`, http.StatusBadRequest},
		{"wildcard bucket", `{"bucket":"*","upstream_url":"http://x","access_key":"AK","secret_key":"S"}`, http.StatusBadRequest},
		{"system bucket", `{"bucket":"__system__","upstream_url":"http://x","access_key":"AK","secret_key":"S"}`, http.StatusBadRequest},
		{"malformed JSON", `{not json`, http.StatusBadRequest},
		{"bucket too short", `{"bucket":"ab","upstream_url":"http://x","access_key":"AK","secret_key":"S"}`, http.StatusBadRequest},
		{"bucket invalid char", `{"bucket":"Foo!","upstream_url":"http://x","access_key":"AK","secret_key":"S"}`, http.StatusBadRequest},
		{"upstream_url too long", fmt.Sprintf(`{"bucket":"valid","upstream_url":"http://%s","access_key":"AK","secret_key":"S"}`, strings.Repeat("a", 2049)), http.StatusBadRequest},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			store := NewStore()
			enc := newTestEncryptor(t)
			api := NewAdminAPI(store, newFakeProposer(), enc)
			r := httptest.NewRequest("POST", "/v1/iam/bucket-upstream", strings.NewReader(c.body))
			r.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			api.HandleBucketUpstreamPut(w, r)
			if w.Code != c.want {
				t.Errorf("status: got %d want %d; body=%s", w.Code, c.want, w.Body.String())
			}
		})
	}
}

func TestAdminAPI_BucketUpstream_GetMasksSecret(t *testing.T) {
	store := NewStore()
	enc := newTestEncryptor(t)
	// A2: AAD prefix in test setup wrap.
	wrapped, _ := WrapSecret(enc, "bucket-upstream:shared", "sk-plain")
	store.applyBucketUpstreamPut(BucketUpstream{
		Bucket: "shared", Endpoint: "http://up", AccessKey: "AKUP",
		SecretKey: "sk-plain", SecretKeyEnc: wrapped,
		CreatedAt: time.Now().UTC(), CreatedBy: "sa-admin",
	})
	api := NewAdminAPI(store, newFakeProposer(), enc)

	r := httptest.NewRequest("GET", "/v1/iam/bucket-upstream/shared", nil)
	w := httptest.NewRecorder()
	api.HandleBucketUpstreamGet(w, r, "shared")

	if w.Code != http.StatusOK {
		t.Fatalf("status: got %d want 200; body=%s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	if strings.Contains(body, "sk-plain") {
		t.Errorf("response leaked plaintext secret: %s", body)
	}
	if !strings.Contains(body, `"access_key":"AKUP"`) {
		t.Errorf("response missing access_key: %s", body)
	}
	// A9: response uses upstream_url, NOT endpoint.
	if !strings.Contains(body, `"upstream_url":"http://up"`) {
		t.Errorf("response missing upstream_url field: %s", body)
	}
	if strings.Contains(body, `"endpoint":`) {
		t.Errorf("response uses obsolete endpoint key: %s", body)
	}

	// Missing → 404.
	r2 := httptest.NewRequest("GET", "/v1/iam/bucket-upstream/none", nil)
	w2 := httptest.NewRecorder()
	api.HandleBucketUpstreamGet(w2, r2, "none")
	if w2.Code != http.StatusNotFound {
		t.Errorf("missing bucket: got %d want 404", w2.Code)
	}
}

func TestAdminAPI_BucketUpstream_List(t *testing.T) {
	store := NewStore()
	enc := newTestEncryptor(t)
	for _, b := range []string{"a", "b"} {
		// A2 prefix
		w, _ := WrapSecret(enc, "bucket-upstream:"+b, "sk-"+b)
		store.applyBucketUpstreamPut(BucketUpstream{
			Bucket: b, Endpoint: "http://up", AccessKey: "AK-" + b,
			SecretKey: "sk-" + b, SecretKeyEnc: w, CreatedAt: time.Now().UTC(),
		})
	}
	api := NewAdminAPI(store, newFakeProposer(), enc)
	r := httptest.NewRequest("GET", "/v1/iam/bucket-upstream", nil)
	w := httptest.NewRecorder()
	api.HandleBucketUpstreamList(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status: got %d want 200", w.Code)
	}
	body := w.Body.String()
	if strings.Contains(body, "sk-a") || strings.Contains(body, "sk-b") {
		t.Errorf("list leaked plaintext secrets: %s", body)
	}
	if !strings.Contains(body, `"AK-a"`) || !strings.Contains(body, `"AK-b"`) {
		t.Errorf("list missing access keys: %s", body)
	}
}

// A7 boil-the-lake: empty list returns [] not 404.
func TestAdminAPI_BucketUpstream_ListEmptyReturnsEmptyArray(t *testing.T) {
	store := NewStore()
	enc := newTestEncryptor(t)
	api := NewAdminAPI(store, newFakeProposer(), enc)
	r := httptest.NewRequest("GET", "/v1/iam/bucket-upstream", nil)
	w := httptest.NewRecorder()
	api.HandleBucketUpstreamList(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status: got %d want 200", w.Code)
	}
	body := strings.TrimSpace(w.Body.String())
	// JSON must be an empty array, not "null" or 404.
	if body != "[]" {
		t.Errorf("empty list body: got %q want %q", body, "[]")
	}
}

func TestAdminAPI_BucketUpstream_Delete(t *testing.T) {
	store := NewStore()
	enc := newTestEncryptor(t)
	wrapped, _ := WrapSecret(enc, "bucket-upstream:shared", "sk")
	store.applyBucketUpstreamPut(BucketUpstream{
		Bucket: "shared", Endpoint: "http://up", AccessKey: "AK",
		SecretKey: "sk", SecretKeyEnc: wrapped, CreatedAt: time.Now().UTC(),
	})
	fp := newFakeProposer()
	api := NewAdminAPI(store, fp, enc)

	r := httptest.NewRequest("DELETE", "/v1/iam/bucket-upstream/shared", nil)
	w := httptest.NewRecorder()
	api.HandleBucketUpstreamDelete(w, r, "shared")
	if w.Code != http.StatusNoContent {
		t.Fatalf("status: got %d want 204", w.Code)
	}
	if len(fp.bucketUpstreamDeletes) != 1 || fp.bucketUpstreamDeletes[0] != "shared" {
		t.Errorf("propose: got %v want [shared]", fp.bucketUpstreamDeletes)
	}

	r2 := httptest.NewRequest("DELETE", "/v1/iam/bucket-upstream/none", nil)
	w2 := httptest.NewRecorder()
	api.HandleBucketUpstreamDelete(w2, r2, "none")
	if w2.Code != http.StatusNotFound {
		t.Errorf("missing: got %d want 404", w2.Code)
	}
}

// A7 boil-the-lake: Propose failure → 500.
func TestAdminAPI_BucketUpstream_ProposeFailureReturns500(t *testing.T) {
	store := NewStore()
	enc := newTestEncryptor(t)
	fp := newFakeProposer()
	fp.bucketUpstreamPutErr = errors.New("simulated propose failure")
	api := NewAdminAPI(store, fp, enc)

	body := strings.NewReader(`{"bucket":"shared","upstream_url":"http://x","access_key":"AK","secret_key":"S"}`)
	r := httptest.NewRequest("POST", "/v1/iam/bucket-upstream", body)
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	api.HandleBucketUpstreamPut(w, r)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("Propose failure: got %d want 500; body=%s", w.Code, w.Body.String())
	}
}

// A7 boil-the-lake: Delete propose failure → 500.
func TestAdminAPI_BucketUpstream_DeleteProposeFailureReturns500(t *testing.T) {
	store := NewStore()
	enc := newTestEncryptor(t)
	wrapped, _ := WrapSecret(enc, "bucket-upstream:shared", "sk")
	store.applyBucketUpstreamPut(BucketUpstream{
		Bucket: "shared", Endpoint: "http://up", AccessKey: "AK",
		SecretKey: "sk", SecretKeyEnc: wrapped, CreatedAt: time.Now().UTC(),
	})
	fp := newFakeProposer()
	fp.bucketUpstreamDeleteErr = errors.New("simulated propose failure")
	api := NewAdminAPI(store, fp, enc)

	r := httptest.NewRequest("DELETE", "/v1/iam/bucket-upstream/shared", nil)
	w := httptest.NewRecorder()
	api.HandleBucketUpstreamDelete(w, r, "shared")

	if w.Code != http.StatusInternalServerError {
		t.Errorf("Propose failure: got %d want 500; body=%s", w.Code, w.Body.String())
	}
}
