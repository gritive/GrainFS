package protocred

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProtocolIcebergRemoved(t *testing.T) {
	// After removal, validProtocol should NOT accept "iceberg"
	assert.False(t, validProtocol(Protocol("iceberg")), "ProtocolIceberg must be removed")
}
