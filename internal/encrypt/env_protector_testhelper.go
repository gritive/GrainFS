package encrypt

// NewEnvProtectorForTest builds an EnvProtector bound to a fixed factor set and a
// static recovery passphrase, with the production random-salt generator. It exists
// so other packages' tests (e.g. transport.Keystore) can exercise the env-protected
// path deterministically without depending on real machine factors. Not for
// production use — production builds the protector from config via
// serveruntime.buildKEKProtector.
func NewEnvProtectorForTest(factors []string, secret string) *EnvProtector {
	return &EnvProtector{
		factors:  staticFactors(factors),
		recovery: func() ([]byte, error) { return []byte(secret), nil },
		saltGen:  randomSalt,
	}
}

type staticFactors []string

func (s staticFactors) Factors() ([]string, error) { return []string(s), nil }
