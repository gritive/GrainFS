package serveruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/iam"
)

// inProcessPropose dispatches IAM cmd payloads directly to the FSM Applier,
// bypassing raft. This mirrors the production MetaProposer wiring (which ships
// payload bytes through raft before reaching the same Applier) and lets tests
// drive the AdminAPI end-to-end without standing up a cluster.
func inProcessPropose(applier *iam.Applier) iam.ProposeFunc {
	return func(_ context.Context, t clusterpb.MetaCmdType, payload []byte) error {
		switch t {
		case clusterpb.MetaCmdTypeIAMSACreate:
			return applier.ApplySACreate(payload)
		case clusterpb.MetaCmdTypeIAMSADelete:
			return applier.ApplySADelete(payload)
		case clusterpb.MetaCmdTypeIAMKeyCreate:
			return applier.ApplyKeyCreate(payload)
		case clusterpb.MetaCmdTypeIAMKeyRevoke:
			return applier.ApplyKeyRevoke(payload)
		case clusterpb.MetaCmdTypeIAMGrantPut:
			return applier.ApplyGrantPut(payload)
		case clusterpb.MetaCmdTypeIAMGrantDelete:
			return applier.ApplyGrantDelete(payload)
		case clusterpb.MetaCmdTypeIAMGrantWildcardPut:
			return applier.ApplyGrantWildcardPut(payload)
		case clusterpb.MetaCmdTypeIAMInitFirstSA:
			return applier.ApplyInitFirstSA(payload)
		default:
			return fmt.Errorf("inProcessPropose: unhandled cmd type %v", t)
		}
	}
}

func newTestEncryptor(t *testing.T) *encrypt.Encryptor {
	t.Helper()
	key := bytes.Repeat([]byte{0x42}, 32)
	enc, err := encrypt.NewEncryptor(key)
	if err != nil {
		t.Fatalf("NewEncryptor: %v", err)
	}
	return enc
}

func startIAMAdminTestServer(t *testing.T, api *iam.AdminAPI) *http.Client {
	t.Helper()
	d, err := os.MkdirTemp("/tmp", "gs-iam-uds-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(d) })
	sock := filepath.Join(d, "a.sock")

	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}

	h := server.New(
		server.WithListener(ln),
		server.WithTransport(standard.NewTransporter),
		server.WithHostPorts(""),
	)
	RegisterIAMAdminRoutes(h, api)

	go h.Spin() //nolint:errcheck
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = h.Shutdown(ctx)
	})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn, dialErr := net.Dial("unix", sock)
		if dialErr == nil {
			conn.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	return &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", sock)
			},
		},
		Timeout: 5 * time.Second,
	}
}

func newAdminAPIWithStore(t *testing.T) (*iam.AdminAPI, *iam.Store) {
	t.Helper()
	enc := newTestEncryptor(t)
	store := iam.NewStore()
	applier := iam.NewApplier(store, enc)
	proposer := &iam.MetaProposer{Propose: inProcessPropose(applier)}
	api := iam.NewAdminAPI(store, proposer, enc)
	return api, store
}

func createSAViaAPI(t *testing.T, cli *http.Client, name string) iam.SACreateResponse {
	t.Helper()
	resp, err := cli.Post("http://unix/v1/iam/sa", "application/json",
		bytes.NewReader([]byte(fmt.Sprintf(`{"name":%q}`, name))))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("create status=%d body=%s", resp.StatusCode, body)
	}
	var out iam.SACreateResponse
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode: %v body=%s", err, body)
	}
	return out
}

func TestRegisterIAMAdminRoutes_SACreateAndList(t *testing.T) {
	api, _ := newAdminAPIWithStore(t)
	cli := startIAMAdminTestServer(t, api)

	created := createSAViaAPI(t, cli, "alice")
	if created.Name != "alice" || created.AccessKey == "" || created.SecretKey == "" {
		t.Fatalf("unexpected response: %+v", created)
	}

	resp, err := cli.Get("http://unix/v1/iam/sa")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("list status=%d body=%s", resp.StatusCode, body)
	}
	var list []iam.SAListItem
	if err := json.Unmarshal(body, &list); err != nil {
		t.Fatalf("decode list: %v body=%s", err, body)
	}
	if len(list) != 1 || list[0].Name != "alice" {
		t.Fatalf("list mismatch: %+v", list)
	}
	if list[0].NumKeys != 1 {
		t.Fatalf("expected 1 key for alice, got %d", list[0].NumKeys)
	}
}

func TestRegisterIAMAdminRoutes_SAGetAndDelete(t *testing.T) {
	api, store := newAdminAPIWithStore(t)
	cli := startIAMAdminTestServer(t, api)

	created := createSAViaAPI(t, cli, "carol")

	resp, err := cli.Get("http://unix/v1/iam/sa/" + created.SAID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("get status=%d body=%s", resp.StatusCode, body)
	}

	req, _ := http.NewRequest("DELETE", "http://unix/v1/iam/sa/"+created.SAID, nil)
	resp, err = cli.Do(req)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("delete status=%d", resp.StatusCode)
	}
	if _, ok := store.LookupSA(created.SAID); ok {
		t.Fatalf("SA not deleted")
	}
}

func TestRegisterIAMAdminRoutes_GrantListQuery(t *testing.T) {
	api, _ := newAdminAPIWithStore(t)
	cli := startIAMAdminTestServer(t, api)

	sa := createSAViaAPI(t, cli, "alice")

	for _, b := range []string{"b1", "b2"} {
		body, _ := json.Marshal(iam.GrantPutRequest{SAID: sa.SAID, Bucket: b, Role: "Read"})
		req, _ := http.NewRequest("PUT", "http://unix/v1/iam/grant", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := cli.Do(req)
		if err != nil {
			t.Fatalf("grant put %s: %v", b, err)
		}
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("grant put %s status=%d body=%s", b, resp.StatusCode, respBody)
		}
	}

	// Query string passes through.
	resp, err := cli.Get("http://unix/v1/iam/grant?sa=" + sa.SAID + "&bucket=b1")
	if err != nil {
		t.Fatalf("grant list: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("list status=%d body=%s", resp.StatusCode, body)
	}
	var items []iam.GrantListItem
	if err := json.Unmarshal(body, &items); err != nil {
		t.Fatalf("decode: %v body=%s", err, body)
	}
	if len(items) != 1 || items[0].Bucket != "b1" {
		t.Fatalf("grant filter mismatch: %+v", items)
	}
}
