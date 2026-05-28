package serveruntime

import (
	"crypto/tls"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBootState_PerNodeCertField asserts the field exists on the struct
// (compile-time guard).
func TestBootState_PerNodeCertField(t *testing.T) {
	st := &bootState{}
	st.perNodeCert = tls.Certificate{} // must compile
	require.IsType(t, tls.Certificate{}, st.perNodeCert)
}
