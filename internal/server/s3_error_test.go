package server

import (
	"encoding/xml"
	"fmt"
	"testing"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
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
