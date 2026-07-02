package transport

import (
	"errors"
	"fmt"
	"testing"

	hzerrors "github.com/cloudwego/hertz/pkg/common/errors"
	"github.com/stretchr/testify/assert"
)

func TestClassifyShardClientErr_NoFreeConnsBecomesLocalBackpressure(t *testing.T) {
	err := classifyShardClientErr(fmt.Errorf("do request: %w", hzerrors.ErrNoFreeConns))
	assert.True(t, errors.Is(err, ErrLocalBackpressure), "pool exhaustion must be tagged as local backpressure")
	assert.True(t, errors.Is(err, hzerrors.ErrNoFreeConns), "original cause must stay unwrappable")
}

func TestClassifyShardClientErr_PassThrough(t *testing.T) {
	cause := errors.New("connection refused")
	err := classifyShardClientErr(cause)
	assert.False(t, errors.Is(err, ErrLocalBackpressure))
	assert.Same(t, cause, err)

	assert.Nil(t, classifyShardClientErr(nil))
}
