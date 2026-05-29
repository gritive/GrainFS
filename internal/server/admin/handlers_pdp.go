package admin

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"

	"github.com/gritive/GrainFS/internal/iam"
	iampdp "github.com/gritive/GrainFS/internal/iam/pdp"
)

// PDPSetToken seals plaintext under the cluster-consistent IAM DEK path and
// proposes it as the reserved iam.pdp.token config key. This is the legitimate
// writer that bypasses the generic config-route reserved-key guard (intended).
// Re-setting the identical token is a no-op so an idempotent CLI invocation
// does not churn the decision cache / PDP client.
func PDPSetToken(ctx context.Context, d *Deps, plaintext string) error {
	if d.PDPTokens == nil || d.PDPTokens.CurrentEncryptor() == nil {
		return fmt.Errorf("pdp set-token: encryptor not ready")
	}
	if cur, _, ok := d.PDPTokens.CurrentToken(); ok && cur == plaintext {
		return nil // no-op on idempotent re-set (avoid cache/client churn)
	}
	enc := d.PDPTokens.CurrentEncryptor()
	wrap := func(saID, accessKey, pt string) ([]byte, uint32, error) {
		return iam.WrapSecret(enc, saID, accessKey, pt)
	}
	envJSON, err := iampdp.SealToken(wrap, plaintext)
	if err != nil {
		return err
	}
	return d.ConfigProposer.ProposeConfigPut(ctx, iampdp.TokenConfigKey, string(envJSON))
}

// PDPClearToken removes the iam.pdp.token override. ProposeConfigDelete reverts
// the key to its registered default (""), which the envelope validator accepts
// as "no token configured" — the desired clear semantic. (A ProposeConfigPut
// of "" would also clear, but Delete is the canonical override-removal path and
// keeps the config store's Set flag accurate.)
func PDPClearToken(ctx context.Context, d *Deps) error {
	if d.ConfigProposer == nil {
		return fmt.Errorf("pdp clear-token: config proposer not ready")
	}
	return d.ConfigProposer.ProposeConfigDelete(ctx, iampdp.TokenConfigKey)
}

// PDPStatus reads the current iam.pdp config + unseals the current token via
// d.PDPTokens, then renders the human-readable status. It NEVER returns the
// token — only token_configured + the SHA-256 fingerprint.
func PDPStatus(_ context.Context, d *Deps) (string, error) {
	if d.ConfigStore == nil {
		return "", fmt.Errorf("pdp show: config store not ready")
	}
	pdpRaw, _ := d.ConfigStore.GetString("iam.pdp")
	var token string
	if d.PDPTokens != nil {
		if tok, _, ok := d.PDPTokens.CurrentToken(); ok {
			token = tok
		}
	}
	return BuildPDPStatus(pdpRaw, token), nil
}

// BuildPDPStatus renders the PDP status text: endpoint, scheme, TLS
// (min_version + ca_pinned), token_configured, and the token fingerprint when a
// token is configured. It NEVER prints the token itself. A malformed iam.pdp
// document degrades gracefully (the parse error is surfaced) rather than
// hiding the token state.
func BuildPDPStatus(pdpRaw, token string) string {
	var b strings.Builder
	cfg, err := iampdp.ParseConfig([]byte(pdpRaw))
	if err != nil {
		fmt.Fprintf(&b, "config_error:    %s\n", err.Error())
	} else {
		fmt.Fprintf(&b, "enabled:         %t\n", cfg.Enabled)
		fmt.Fprintf(&b, "endpoint:        %s\n", cfg.RemoteURL)
		fmt.Fprintf(&b, "scheme:          %s\n", cfg.Scheme)
		fmt.Fprintf(&b, "tls.min_version: %s\n", tlsVersionString(cfg.TLS.MinVersion))
		fmt.Fprintf(&b, "tls.ca_pinned:   %t\n", cfg.TLS.CAPEM != "")
	}
	fmt.Fprintf(&b, "token_configured: %t\n", token != "")
	if token != "" {
		fmt.Fprintf(&b, "token_fingerprint: %s\n", iampdp.Fingerprint(token))
	}
	return b.String()
}

func tlsVersionString(v uint16) string {
	switch v {
	case tls.VersionTLS12:
		return "1.2"
	case tls.VersionTLS13:
		return "1.3"
	default:
		return fmt.Sprintf("0x%04x", v)
	}
}
