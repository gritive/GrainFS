package server

import (
	"encoding/xml"
	"fmt"
	"testing"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage"
)

// TestMapError_DEKGenUnknownIsServiceUnavailable: an error wrapping
// encrypt.ErrDEKGenUnknown must be mapped to a retriable 503
// ServiceUnavailable, not a hard 500, so clients retry.
func TestMapError_DEKGenUnknownIsServiceUnavailable(t *testing.T) {
	c := app.NewContext(0)

	err := fmt.Errorf("put 7: data shards unavailable: %w", encrypt.ErrDEKGenUnknown)
	mapError(c, err)

	require.Equal(t, consts.StatusServiceUnavailable, c.Response.StatusCode())

	var got s3Error
	require.NoError(t, xml.Unmarshal(c.Response.Body(), &got))
	assert.Equal(t, "ServiceUnavailable", got.Code)
}

// TestMapError_StalePlacementIsRetryableSlowDown: a persistent CAS loss that
// surfaces as ErrStalePlacement must map to 503 SlowDown, not 500 InternalError.
// S3 clients auto-retry SlowDown; they do not retry 500.
func TestMapError_StalePlacementIsRetryableSlowDown(t *testing.T) {
	c := app.NewContext(0)

	err := fmt.Errorf("append object: %w", cluster.ErrStalePlacement)
	mapError(c, err)

	require.Equal(t, consts.StatusServiceUnavailable, c.Response.StatusCode())

	var got s3Error
	require.NoError(t, xml.Unmarshal(c.Response.Body(), &got))
	assert.Equal(t, "SlowDown", got.Code)
}

// TestMapError_ProposeTimeoutIsRetryableSlowDown: a propose that exhausts its
// raft-commit deadline under load must map to a retryable 503 SlowDown, not a
// fatal 500. S3 clients auto-retry SlowDown; they do not retry 500. This is the
// CompleteMultipartUpload-under-load 500 fix.
func TestMapError_ProposeTimeoutIsRetryableSlowDown(t *testing.T) {
	c := app.NewContext(0)

	err := fmt.Errorf("complete multipart: %w",
		fmt.Errorf("propose: forward timed out: %w", cluster.ErrProposeTimeout))
	mapError(c, err)

	require.Equal(t, consts.StatusServiceUnavailable, c.Response.StatusCode())

	var got s3Error
	require.NoError(t, xml.Unmarshal(c.Response.Body(), &got))
	assert.Equal(t, "SlowDown", got.Code)
}

// TestMapError_ContentMD5MismatchIsBadDigest: a body whose MD5 does not match
// the client Content-MD5 must map to 400 BadDigest, not a 500. Identical on
// every node (direct or forwarded — both end in storage.ErrContentMD5Mismatch).
func TestMapError_ContentMD5MismatchIsBadDigest(t *testing.T) {
	c := app.NewContext(0)

	err := fmt.Errorf("put object: %w", storage.ErrContentMD5Mismatch)
	mapError(c, err)

	require.Equal(t, consts.StatusBadRequest, c.Response.StatusCode())

	var got s3Error
	require.NoError(t, xml.Unmarshal(c.Response.Body(), &got))
	assert.Equal(t, "BadDigest", got.Code)
}

// TestMapError_InvalidDigestIsBadRequest: a malformed Content-MD5 maps to 400
// InvalidDigest (distinct from BadDigest, which is a valid-but-wrong digest).
func TestMapError_InvalidDigestIsBadRequest(t *testing.T) {
	c := app.NewContext(0)

	err := fmt.Errorf("parse: %w", storage.ErrInvalidDigest)
	mapError(c, err)

	require.Equal(t, consts.StatusBadRequest, c.Response.StatusCode())

	var got s3Error
	require.NoError(t, xml.Unmarshal(c.Response.Body(), &got))
	assert.Equal(t, "InvalidDigest", got.Code)
}

// TestMapError_GenericErrorIsInternalError: a plain error that matches no
// sentinel must still map to 500 InternalError (no over-tagging).
func TestMapError_GenericErrorIsInternalError(t *testing.T) {
	c := app.NewContext(0)

	mapError(c, fmt.Errorf("some generic failure"))

	require.Equal(t, consts.StatusInternalServerError, c.Response.StatusCode())

	var got s3Error
	require.NoError(t, xml.Unmarshal(c.Response.Body(), &got))
	assert.Equal(t, "InternalError", got.Code)
}
