package admin

import (
	"testing"

	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/stretchr/testify/require"
)

func TestStatusForExecutionCodes(t *testing.T) {
	require.Equal(t, consts.StatusGatewayTimeout, statusForCode("job_timeout"))
	require.Equal(t, consts.StatusConflict, statusForCode("job_cancelled"))
	require.Equal(t, consts.StatusInternalServerError, statusForCode("job_failed"))
	require.Equal(t, consts.StatusInternalServerError, statusForCode("aggregation_failed"))
}
