package server

import (
	"bytes"
	"io"
	"testing"

	"github.com/cloudwego/hertz/pkg/app"
)

func TestPutObjectBodyReader_UsesStreamingRequestBody(t *testing.T) {
	payload := []byte("streaming upload body")
	body := &countingRequestBody{Reader: bytes.NewReader(payload)}
	c := app.NewContext(0)
	c.Request.SetBodyStream(body, len(payload))

	r, err := putObjectPayloadReader(c)
	if err != nil {
		t.Fatalf("putObjectPayloadReader: %v", err)
	}
	if body.reads != 0 {
		t.Fatalf("putObjectPayloadReader read streaming body during setup %d times", body.reads)
	}

	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("body: got %q, want %q", got, payload)
	}
}

func TestPutObjectBodyReader_DecodesAWSChunkedStreamingBody(t *testing.T) {
	input := []byte("5;chunk-signature=abc\r\nhello\r\n6;chunk-signature=def\r\n world\r\n0;chunk-signature=final\r\n\r\n")
	body := &countingRequestBody{Reader: bytes.NewReader(input)}
	c := app.NewContext(0)
	c.Request.SetBodyStream(body, len(input))
	c.Request.Header.Set("Content-Encoding", "aws-chunked")

	r, err := putObjectPayloadReader(c)
	if err != nil {
		t.Fatalf("putObjectPayloadReader: %v", err)
	}
	if body.reads != 0 {
		t.Fatalf("putObjectPayloadReader read streaming body during setup %d times", body.reads)
	}

	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	if string(got) != "hello world" {
		t.Fatalf("body: got %q, want %q", got, "hello world")
	}
}
