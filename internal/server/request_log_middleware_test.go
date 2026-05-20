package server

import (
	"bytes"
	"io"
	"testing"

	"github.com/cloudwego/hertz/pkg/app"
)

type countingRequestBody struct {
	*bytes.Reader
	reads int
}

func (r *countingRequestBody) Read(p []byte) (int, error) {
	r.reads++
	return r.Reader.Read(p)
}

func TestS3RequestBytesIn_DoesNotDrainStreamingBody(t *testing.T) {
	payload := []byte("streaming upload body")
	body := &countingRequestBody{Reader: bytes.NewReader(payload)}
	c := app.NewContext(0)
	c.Request.SetBodyStream(body, len(payload))

	if got := s3RequestBytesIn(c); got != int64(len(payload)) {
		t.Fatalf("bytes_in: got %d, want %d", got, len(payload))
	}
	if body.reads != 0 {
		t.Fatalf("s3RequestBytesIn read streaming request body %d times", body.reads)
	}

	got, err := io.ReadAll(c.Request.BodyStream())
	if err != nil {
		t.Fatalf("read body stream: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("body stream was altered: got %q, want %q", got, payload)
	}
}

func TestAuditEnvelopeBytesIn_DoesNotDrainStreamingBody(t *testing.T) {
	payload := []byte("streaming upload body")
	body := &countingRequestBody{Reader: bytes.NewReader(payload)}
	c := app.NewContext(0)
	c.Request.SetMethod("PUT")
	c.Request.SetRequestURI("/bucket/key")
	c.Request.SetBodyStream(body, len(payload))

	s := &Server{}
	ev := s.newAuditEnvelopeEvent(t.Context(), c, auditEnvelopeInput{
		bucket:    "bucket",
		key:       "key",
		requestID: "req",
	})

	if ev.BytesIn != int64(len(payload)) {
		t.Fatalf("bytes_in: got %d, want %d", ev.BytesIn, len(payload))
	}
	if body.reads != 0 {
		t.Fatalf("newAuditEnvelopeEvent read streaming request body %d times", body.reads)
	}
}

func TestAuditAuthFailureBytesIn_DoesNotDrainStreamingBody(t *testing.T) {
	payload := []byte("streaming upload body")
	body := &countingRequestBody{Reader: bytes.NewReader(payload)}
	c := app.NewContext(0)
	c.Request.SetMethod("PUT")
	c.Request.SetRequestURI("/bucket/key")
	c.Request.SetBodyStream(body, len(payload))

	s := &Server{}
	ev := s.newAuditAuthFailureEvent(t.Context(), c, "bucket", "key", "req", 403, "authn")

	if ev.BytesIn != int64(len(payload)) {
		t.Fatalf("bytes_in: got %d, want %d", ev.BytesIn, len(payload))
	}
	if body.reads != 0 {
		t.Fatalf("newAuditAuthFailureEvent read streaming request body %d times", body.reads)
	}
}
