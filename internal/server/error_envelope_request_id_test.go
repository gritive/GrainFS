// Error envelope request_id propagation (auth-redesign §5 T42).
//
// writeXMLError and writeIcebergError must embed the X-GrainFS-Request-Id
// value from the Hertz request context's K/V store (stashed there by
// WithRequestID) into the response body, so clients can correlate failures
// with server-side audit/log records without trusting only the response
// header.
package server

import (
	"strings"
	"testing"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestS3Error_IncludesRequestID: when WithRequestID has stashed a rid on the
// Hertz context, writeXMLError must include it as <RequestId>...</RequestId>
// inside the <Error> body.
func TestS3Error_IncludesRequestID(t *testing.T) {
	const rid = "rid-42"

	c := app.NewContext(0)
	c.Set(requestIDHertzKey, rid)

	writeXMLError(c, 404, "NoSuchBucket", "The specified bucket does not exist")

	body := string(c.Response.Body())
	require.NotEmpty(t, body, "response body must not be empty")
	assert.Contains(t, body, "<RequestId>"+rid+"</RequestId>",
		"S3 XML error must embed <RequestId> from request context")
	// Sanity: the other fields are still present so we didn't break the envelope.
	assert.Contains(t, body, "<Code>NoSuchBucket</Code>")
	assert.Contains(t, body, "<Message>The specified bucket does not exist</Message>")
}

// TestS3Error_OmitsEmptyRequestID: when no rid is set (e.g. middleware did not
// run in a unit-test path), the envelope must NOT emit an empty <RequestId/>
// element — that would mislead clients into thinking the server returned a
// blank rid.
func TestS3Error_OmitsEmptyRequestID(t *testing.T) {
	c := app.NewContext(0)
	// No c.Set for the rid key — simulates WithRequestID skipped.

	writeXMLError(c, 500, "InternalError", "boom")
	body := string(c.Response.Body())
	assert.NotContains(t, body, "<RequestId",
		"missing rid must omit the <RequestId> element entirely, not emit an empty one")
}

// TestIcebergError_IncludesRequestID: writeIcebergError must include a
// top-level "request_id" JSON field alongside "error" when the Hertz context
// carries one.
func TestIcebergError_IncludesRequestID(t *testing.T) {
	const rid = "rid-99"

	c := app.NewContext(0)
	c.Set(requestIDHertzKey, rid)

	writeIcebergError(c, 401, "NotAuthorizedException", "missing bearer token")

	body := string(c.Response.Body())
	require.NotEmpty(t, body, "response body must not be empty")
	// JSON field check tolerates either "request_id":"rid-99" or with a space.
	hasField := strings.Contains(body, `"request_id":"`+rid+`"`) ||
		strings.Contains(body, `"request_id": "`+rid+`"`)
	assert.True(t, hasField, "Iceberg JSON error must contain request_id; got %q", body)
	assert.Contains(t, body, `"NotAuthorizedException"`)
	assert.Contains(t, body, `"missing bearer token"`)
}

// TestIcebergError_OmitsEmptyRequestID: when no rid is on the context, the
// JSON envelope must not contain a "request_id" key at all (rather than an
// empty string), matching the S3 omitempty behaviour.
func TestIcebergError_OmitsEmptyRequestID(t *testing.T) {
	c := app.NewContext(0)

	writeIcebergError(c, 500, "InternalServerError", "boom")
	body := string(c.Response.Body())
	assert.NotContains(t, body, `"request_id"`,
		"missing rid must omit the request_id field entirely")
}
