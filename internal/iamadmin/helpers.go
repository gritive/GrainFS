package iamadmin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
)

// dialClient builds a Client from opts. If opts.Endpoint already looks like
// an http(s) URL (test seam), bypass ResolveEndpoint so tests against
// httptest.Server work without environment setup.
func dialClient(opts BaseOptions) (*Client, error) {
	if strings.HasPrefix(opts.Endpoint, "http://") || strings.HasPrefix(opts.Endpoint, "https://") {
		return NewClientForURL(opts.Endpoint), nil
	}
	return NewClient(opts.Endpoint)
}

// withTimeout returns ctx unchanged if d <= 0, otherwise applies a deadline.
// Mirrors volumeadmin's withTimeout semantic: zero means "no cap".
func withTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, d)
}

// emitJSON serializes v to w with a trailing newline. Used for JSON-mode
// outputs where the response is a typed struct. Distinct from
// WriteRawWithNewline, which preserves the server body verbatim for raw
// passthrough commands.
func emitJSON(w io.Writer, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	_, err = fmt.Fprintln(w, string(b))
	return err
}
