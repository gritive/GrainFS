package iamadmin

import (
	"bytes"
	"testing"
	"time"
)

func TestFormat_SACreateText(t *testing.T) {
	var buf bytes.Buffer
	RenderSACreateText(&buf, SACreateResponse{
		SAID: "sa-x", Name: "alice", AccessKey: "AK1", SecretKey: "SK1",
	})
	want := "Created service account alice\n" +
		"sa_id:       sa-x\n" +
		"access_key:  AK1\n" +
		"secret_key:  SK1\n" +
		"Store the secret_key now — it will not be shown again.\n"
	if got := buf.String(); got != want {
		t.Errorf("got:\n%q\nwant:\n%q", got, want)
	}
}

func TestFormat_SAListText(t *testing.T) {
	var buf bytes.Buffer
	items := []SAListItem{
		{SAID: "sa-1", Name: "alice", NumKeys: 2, NumGrants: 3,
			CreatedAt: time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC)},
	}
	RenderSAListText(&buf, items)
	want := "SA ID  NAME   KEYS  GRANTS  CREATED AT\n" +
		"sa-1   alice  2     3       2026-05-20T12:00:00Z\n"
	if got := buf.String(); got != want {
		t.Errorf("got:\n%q\nwant:\n%q", got, want)
	}
}

func TestFormat_SAGetText(t *testing.T) {
	var buf bytes.Buffer
	RenderSAGetText(&buf, SAGetResponse{
		SAID: "sa-x", Name: "alice", Description: "data team",
		CreatedAt: time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC),
	})
	want := "SA ID  NAME   DESCRIPTION  CREATED AT\n" +
		"sa-x   alice  data team    2026-05-20T12:00:00Z\n"
	if got := buf.String(); got != want {
		t.Errorf("got:\n%q\nwant:\n%q", got, want)
	}
}

func TestFormat_SADeleteText(t *testing.T) {
	var buf bytes.Buffer
	RenderSADeleteText(&buf, "sa-x")
	if got := buf.String(); got != "Deleted service account sa-x\n" {
		t.Errorf("got %q", got)
	}
}

func TestFormat_PassthroughJSON(t *testing.T) {
	var buf bytes.Buffer
	WriteRawWithNewline(&buf, []byte(`{"foo":"bar"}`))
	if got := buf.String(); got != `{"foo":"bar"}`+"\n" {
		t.Errorf("got %q", got)
	}
}
