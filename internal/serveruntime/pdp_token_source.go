package serveruntime

import (
	"crypto/sha256"
	"encoding/hex"
	"sync/atomic"

	"github.com/gritive/GrainFS/internal/iam"
	iampdp "github.com/gritive/GrainFS/internal/iam/pdp"
	"github.com/gritive/GrainFS/internal/storage"
)

// configGetter is the slim config read the token source needs (*config.Store satisfies it).
type configGetter interface {
	GetString(key string) (string, bool)
}

// pdpTokenSource implements iampdp.TokenSource AND admin.PDPTokenManager. It reads
// the sealed envelope from the config store per call and unseals it with the LIVE
// encryptor (atomic.Pointer, updated by wireIAMEncryptor on snapshot-restore swaps).
// gen = hash of the sealed envelope string, so any reseal/rotation/clear changes gen.
type pdpTokenSource struct {
	cfg configGetter
	enc atomic.Pointer[storage.DataEncryptor]
}

func newPDPTokenSource(cfg configGetter) *pdpTokenSource { return &pdpTokenSource{cfg: cfg} }

func (s *pdpTokenSource) setEncryptor(enc storage.DataEncryptor) { s.enc.Store(&enc) }

// CurrentEncryptor returns the live encryptor (for the admin set-token handler). nil if not ready.
func (s *pdpTokenSource) CurrentEncryptor() storage.DataEncryptor {
	if p := s.enc.Load(); p != nil {
		return *p
	}
	return nil
}

// CurrentToken reads + unseals the configured bearer token. ok=false ⇒ none configured / not ready.
func (s *pdpTokenSource) CurrentToken() (string, string, bool) {
	raw, ok := s.cfg.GetString(iampdp.TokenConfigKey)
	if !ok || raw == "" {
		return "", "", false
	}
	env, err := iampdp.ParseTokenEnvelope([]byte(raw))
	if err != nil {
		return "", "", false
	}
	encp := s.enc.Load()
	if encp == nil {
		return "", "", false
	}
	tok, err := iampdp.OpenToken(func(sa, ak string, gen uint32, ct []byte) (string, error) {
		return iam.UnwrapSecret(*encp, sa, ak, gen, ct)
	}, env)
	if err != nil {
		return "", "", false
	}
	sum := sha256.Sum256([]byte(raw))
	return tok, hex.EncodeToString(sum[:])[:16], true
}
