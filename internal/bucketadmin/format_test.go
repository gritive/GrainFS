package bucketadmin

import (
	"bytes"
	"strings"
	"testing"
)

func TestFormat_CreateText(t *testing.T) {
	var buf bytes.Buffer
	RenderCreateText(&buf, "my-bucket")
	want := "Created bucket my-bucket\n"
	if got := buf.String(); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestFormat_ListText(t *testing.T) {
	var buf bytes.Buffer
	items := []ListItem{
		{Name: "alpha", HasUpstream: true},
		{Name: "bravo", HasUpstream: false},
	}
	if err := RenderListText(&buf, items); err != nil {
		t.Fatal(err)
	}
	// tabwriter (0,0,2,' ',0): column 1 max="alpha"/"bravo"/"NAME"=5; pad +2 → 7 spaces.
	want := "NAME   HAS_UPSTREAM\n" +
		"alpha  yes\n" +
		"bravo  no\n"
	if got := buf.String(); got != want {
		t.Errorf("got:\n%q\nwant:\n%q", got, want)
	}
}

func TestFormat_DeleteText(t *testing.T) {
	var buf bytes.Buffer
	RenderDeleteText(&buf, "my-bucket")
	want := "Deleted bucket my-bucket\n"
	if got := buf.String(); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestFormat_InfoText_AllFieldsPresent(t *testing.T) {
	var buf bytes.Buffer
	count := int64(42)
	r := InfoResponse{
		Name:        "my-bucket",
		ObjectCount: &count,
		HasUpstream: true,
		Versioning:  "Enabled",
	}
	if err := RenderInfoText(&buf, r); err != nil {
		t.Fatal(err)
	}
	got := buf.String()
	// Sanity checks that exercise the conditional branches.
	if !strings.Contains(got, "my-bucket") {
		t.Errorf("missing bucket name: %q", got)
	}
	if !strings.Contains(got, "42") {
		t.Errorf("missing object count: %q", got)
	}
	if !strings.Contains(got, "Enabled") {
		t.Errorf("missing versioning: %q", got)
	}
	if !strings.Contains(got, "yes") {
		t.Errorf("missing upstream yes: %q", got)
	}
	if !strings.HasPrefix(got, "NAME") {
		t.Errorf("header missing: %q", got)
	}
}

func TestFormat_InfoText_NilObjectCount(t *testing.T) {
	var buf bytes.Buffer
	r := InfoResponse{
		Name:        "my-bucket",
		ObjectCount: nil,
		HasUpstream: false,
		Versioning:  "Suspended",
	}
	if err := RenderInfoText(&buf, r); err != nil {
		t.Fatal(err)
	}
	got := buf.String()
	if !strings.Contains(got, "<unknown>") {
		t.Errorf("expected <unknown>, got %q", got)
	}
	if !strings.Contains(got, "no") {
		t.Errorf("expected upstream no, got %q", got)
	}
}

func TestFormat_InfoText_EmptyVersioning(t *testing.T) {
	var buf bytes.Buffer
	count := int64(0)
	r := InfoResponse{
		Name:        "my-bucket",
		ObjectCount: &count,
		HasUpstream: false,
		Versioning:  "",
	}
	if err := RenderInfoText(&buf, r); err != nil {
		t.Fatal(err)
	}
	got := buf.String()
	// "-" must appear in the versioning column (between two columns of spaces).
	if !strings.Contains(got, " -  ") && !strings.Contains(got, "  -  ") {
		t.Errorf("expected versioning dash, got %q", got)
	}
}

func TestFormat_VersioningGetText_Enabled(t *testing.T) {
	var buf bytes.Buffer
	if err := RenderVersioningGetText(&buf, "my-bucket", VersioningStatus{Status: "Enabled"}); err != nil {
		t.Fatal(err)
	}
	// tabwriter (0,0,2,' ',0): col1 max=len("my-bucket")=9, pad +2 → 11.
	want := "BUCKET     VERSIONING\n" +
		"my-bucket  Enabled\n"
	if got := buf.String(); got != want {
		t.Errorf("got:\n%q\nwant:\n%q", got, want)
	}
}

func TestFormat_VersioningGetText_Suspended(t *testing.T) {
	var buf bytes.Buffer
	if err := RenderVersioningGetText(&buf, "my-bucket", VersioningStatus{Status: "Suspended"}); err != nil {
		t.Fatal(err)
	}
	got := buf.String()
	if !strings.Contains(got, "Suspended") {
		t.Errorf("expected Suspended, got %q", got)
	}
	if !strings.Contains(got, "my-bucket") {
		t.Errorf("expected bucket name, got %q", got)
	}
}

func TestFormat_VersioningGetText_Empty(t *testing.T) {
	var buf bytes.Buffer
	if err := RenderVersioningGetText(&buf, "my-bucket", VersioningStatus{Status: ""}); err != nil {
		t.Fatal(err)
	}
	got := buf.String()
	if !strings.Contains(got, "Off") {
		t.Errorf("expected Off, got %q", got)
	}
}

func TestFormat_VersioningEnableText(t *testing.T) {
	var buf bytes.Buffer
	RenderVersioningEnableText(&buf, "my-bucket")
	want := "Versioning enabled for bucket my-bucket\n"
	if got := buf.String(); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestFormat_VersioningSuspendText(t *testing.T) {
	var buf bytes.Buffer
	RenderVersioningSuspendText(&buf, "my-bucket")
	want := "Versioning suspended for bucket my-bucket\n"
	if got := buf.String(); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestFormat_PassthroughJSON(t *testing.T) {
	var buf bytes.Buffer
	body := []byte(`{"name":"b","detail":"opaque"}`)
	WriteRawWithNewline(&buf, body)
	want := `{"name":"b","detail":"opaque"}` + "\n"
	if got := buf.String(); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}
