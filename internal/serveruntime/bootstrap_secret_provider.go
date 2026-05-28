package serveruntime

import (
	"fmt"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
)

// bootstrapSecretProvider assembles the secret plaintext the zero-CA invite
// handler seals to a joining node: every KEK generation in the cluster KEKStore
// and the transport PSK. cluster.id is NOT included — it is public and travels
// in the InviteBundle. The static encryption.key is intentionally excluded from
// new bootstrap payloads; legacy payload decode remains supported in cluster.
//
// It implements cluster.BootstrapSecretProvider (interface defined at the
// cluster use-site per repo convention).
type bootstrapSecretProvider struct {
	kekStore     *encrypt.KEKStore
	transportPSK string
}

// newBootstrapSecretProvider snapshots the secret material off bootState. The
// KEKStore is populated by the encryption/DEK boot phases; transportPSK by the
// transport phase.
func newBootstrapSecretProvider(state *bootState) *bootstrapSecretProvider {
	return &bootstrapSecretProvider{
		kekStore:     state.kekStore,
		transportPSK: state.transportPSK,
	}
}

// BootstrapSecrets returns every KEK generation and the transport PSK. Each
// returned slice is a fresh copy owned by the caller.
func (p *bootstrapSecretProvider) BootstrapSecrets() (kekGens []cluster.KEKGen, transportPSK []byte, err error) {
	if p.kekStore == nil {
		return nil, nil, fmt.Errorf("bootstrap secrets: KEK store not wired")
	}
	versions := p.kekStore.Versions()
	kekGens = make([]cluster.KEKGen, 0, len(versions))
	for _, v := range versions {
		key, err := p.kekStore.Get(v)
		if err != nil {
			return nil, nil, fmt.Errorf("bootstrap secrets: get KEK gen %d: %w", v, err)
		}
		kekGens = append(kekGens, cluster.KEKGen{Gen: v, Key: key})
	}
	return kekGens, []byte(p.transportPSK), nil
}
