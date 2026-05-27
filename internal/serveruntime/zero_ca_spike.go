package serveruntime

// THROWAWAY feasibility spike (Zero-CA network-path de-risk). Every line in
// this file is dead unless GRAINFS_ZEROCA_SPIKE=1. It exists ONLY to prove that
// a node holding NO cluster secrets can boot to a running server using an invite
// + the encrypt.SealToPeer primitive. NOT production code; do not build on it.
//
// Wire framing here is JSON over a plain TCP socket — this is the operator-style
// out-of-band spike channel, NOT the internal cluster wire (which is FlatBuffers
// over QUIC). It is fine to be ugly: it is deleted when the real network-path
// slice lands.
//
// Scope: the joiner boots as its OWN genesis solo cluster using the leader's
// secrets. It is NOT a raft member of the leader. That is sufficient to prove
// the secret-delivery journey (criteria d/e of the spike plan).
//
// Boot gates this bypasses (fresh-read line numbers, 2026-05-27):
//   - run_from_options.go:~63  LoadOrCreateEncryptionKey (earliest) — staged
//     encryption.key on disk satisfies it.
//   - boot_phases.go:~89       bootValidateConfig --cluster-key — opts.ClusterKey
//     set in memory satisfies it.
//   - dek_keeper_wiring.go:~70/~105  wireDEKKeeper KEK + cluster.id — joiner has
//     NO .join-pending / NO peers / empty meta+raft → isGenesisBoot==true →
//     LoadOrInitKEKStoreDir + LoadOrInitClusterID LOAD the staged keys/0.key and
//     cluster.id rather than regenerating.
//
// KNOWN GAP (must be resolved by the real network-path slice): the invite
// transcript here is signed with ClusterID==nil on BOTH sides, because cluster.id
// is the very secret being bootstrapped. The production meta_join.Handle REQUIRES
// a non-empty r.clusterID and binds it into the transcript (cross-cluster replay
// defense). The slice must either ship cluster.id in the InviteBundle out-of-band
// (it is not a secret) or accept a documented bootstrap exception. This spike does
// NOT exercise that binding.

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/nodeconfig"
)

const (
	spikeEnvOn         = "GRAINFS_ZEROCA_SPIKE"
	spikeEnvListenAddr = "GRAINFS_ZEROCA_SPIKE_ADDR"        // leader: addr to listen on
	spikeEnvLeaderAddr = "GRAINFS_ZEROCA_SPIKE_LEADER_ADDR" // joiner: leader spike addr to dial
	spikeEnvInvite     = "GRAINFS_ZEROCA_SPIKE_INVITE"      // joiner: invite bundle token
	spikeEnvNodeID     = "GRAINFS_ZEROCA_SPIKE_NODE_ID"     // joiner: node id used in transcript
	spikeInviteFile    = "spike-invite.token"               // leader: where the minted bundle token is written
	spikeContextInfo   = "grainfs-zeroca-spike-v0"
)

func spikeEnabled() bool { return os.Getenv(spikeEnvOn) == "1" }

// spikeJoinRequest is the joiner -> leader request (JSON, throwaway).
type spikeJoinRequest struct {
	NodeID    string `json:"node_id"`
	Address   string `json:"address"`
	SPKI      []byte `json:"spki"`
	CertDER   []byte `json:"cert_der"`
	NodeSig   []byte `json:"node_sig"`
	InviteSig []byte `json:"invite_sig"`
	InviteID  string `json:"invite_id"`
	Nonce     []byte `json:"nonce"`
}

// spikeSecrets is the cleartext payload sealed to the joiner's pubkey.
type spikeSecrets struct {
	EncryptionKey []byte `json:"encryption_key"`
	KEKv0         []byte `json:"kek_v0"`
	ClusterID     []byte `json:"cluster_id"`
	ClusterKey    string `json:"cluster_key"`
}

// spikeSealedReply wraps a SealToPeer blob for the wire.
type spikeSealedReply struct {
	EphemeralPub []byte `json:"ephemeral_pub"`
	Ciphertext   []byte `json:"ciphertext"`
}

// --- leader side ----------------------------------------------------------

// maybeStartSpikeLeader, when the spike is enabled and a listen addr is set,
// mints an Ed25519 invite, writes the bundle token to <dataDir>/spike-invite.token
// for the test/operator to hand to the joiner, and starts a TCP listener that
// serves the secret-delivery RPC. It returns immediately; the listener runs in
// the background until process exit.
func maybeStartSpikeLeader(opts ServeOptions) {
	if !spikeEnabled() {
		return
	}
	// A joiner (GRAINFS_ZEROCA_SPIKE_LEADER_ADDR set) is not a leader; do not
	// stand up a secret-delivery listener / mint an invite on it.
	if os.Getenv(spikeEnvLeaderAddr) != "" {
		return
	}
	addr := os.Getenv(spikeEnvListenAddr)
	if addr == "" {
		return
	}
	// The leader needs its secrets on disk. RunFromOptions writes encryption.key
	// (gate 1) and wireDEKKeeper writes keys/0.key + cluster.id during Run, but
	// the listener reads them lazily per-request so it does not race boot.
	invitePriv, invitePub, inviteID, err := cluster.MintInviteKeypair()
	if err != nil {
		log.Error().Err(err).Msg("zeroca-spike: mint invite failed")
		return
	}
	bundle := cluster.InviteBundle{
		InvitePriv: invitePriv,
		InviteID:   inviteID,
		// ClusterID is filled by the joiner-side transcript from the sealed
		// secrets; for the bundle we just need a non-empty placeholder so the
		// decode validation passes. The real cluster.id is delivered sealed.
		ClusterID: "spike",
	}
	token := cluster.EncodeInviteBundle(bundle)
	tokenPath := filepath.Join(opts.DataDir, spikeInviteFile)
	if err := os.WriteFile(tokenPath, []byte(token), 0o600); err != nil {
		log.Error().Err(err).Msg("zeroca-spike: write invite token failed")
		return
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Error().Err(err).Str("addr", addr).Msg("zeroca-spike: listen failed")
		return
	}
	log.Warn().Str("addr", addr).Str("invite_id", inviteID).Msg("zeroca-spike: leader secret-delivery listener up (THROWAWAY)")

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go spikeHandleConn(conn, opts, invitePub, inviteID)
		}
	}()
}

func spikeHandleConn(conn net.Conn, opts ServeOptions, invitePub []byte, inviteID string) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

	var req spikeJoinRequest
	if err := spikeReadFrame(conn, &req); err != nil {
		log.Warn().Err(err).Msg("zeroca-spike: read request failed")
		return
	}

	// Load the leader's secrets from disk now (they exist post-boot).
	cfg := nodeconfig.New(opts.DataDir)
	encKeyPath := opts.EncryptionKeyFile
	if encKeyPath == "" {
		encKeyPath = filepath.Join(opts.DataDir, "encryption.key")
	}
	encKey, err := os.ReadFile(encKeyPath)
	if err != nil {
		log.Warn().Err(err).Msg("zeroca-spike: read encryption.key failed")
		return
	}
	kekv0, err := os.ReadFile(filepath.Join(cfg.KEKDir(), "0.key"))
	if err != nil {
		log.Warn().Err(err).Msg("zeroca-spike: read keys/0.key failed")
		return
	}
	clusterID, err := cfg.LoadClusterID()
	if err != nil {
		log.Warn().Err(err).Msg("zeroca-spike: load cluster.id failed")
		return
	}

	// Invite-gate verification — SAME logic as cluster.MetaJoinReceiver.Handle:
	// SPKI must equal sha256(cert SPKI), both signatures verify over the
	// canonical transcript (clusterID is the REAL leader cluster.id; the joiner
	// rebuilds the same transcript because it learns the value... but it cannot
	// know it before receiving the secret. The spike binds the transcript to the
	// nonce + node identity only, with an empty clusterID on BOTH sides, since
	// clusterID delivery is the very thing being bootstrapped here.)
	leaf, err := x509.ParseCertificate(req.CertDER)
	if err != nil {
		log.Warn().Err(err).Msg("zeroca-spike: bad cert")
		return
	}
	var spki [32]byte
	copy(spki[:], req.SPKI)
	if sha256.Sum256(leaf.RawSubjectPublicKeyInfo) != spki {
		log.Warn().Msg("zeroca-spike: SPKI does not match cert")
		return
	}
	ecPub, ok := leaf.PublicKey.(*ecdsa.PublicKey)
	if !ok {
		log.Warn().Msg("zeroca-spike: non-ECDSA node key")
		return
	}
	tr := encrypt.InviteTranscript{
		ClusterID: nil, // bootstrap: cluster.id is delivered sealed, not pre-shared
		Nonce:     req.Nonce,
		NodeID:    req.NodeID,
		Address:   req.Address,
		SPKI:      req.SPKI,
		Bind:      nil,
	}
	if req.InviteID != inviteID {
		log.Warn().Msg("zeroca-spike: unknown invite id")
		return
	}
	if !encrypt.VerifyInviteTranscript(invitePub, tr, req.InviteSig) {
		log.Warn().Msg("zeroca-spike: invite signature invalid")
		return
	}
	if !encrypt.VerifyNodeTranscript(ecPub, tr, req.NodeSig) {
		log.Warn().Msg("zeroca-spike: node signature invalid")
		return
	}

	// Seal the secrets to the joiner's node identity key (ecPub).
	secrets := spikeSecrets{
		EncryptionKey: encKey,
		KEKv0:         kekv0,
		ClusterID:     clusterID,
		ClusterKey:    opts.ClusterKey,
	}
	plain, _ := json.Marshal(secrets)
	aad := append([]byte(req.NodeID), req.SPKI...)
	sealed, err := encrypt.SealToPeer(ecPub, plain, []byte(spikeContextInfo), aad)
	if err != nil {
		log.Warn().Err(err).Msg("zeroca-spike: SealToPeer failed")
		return
	}
	reply := spikeSealedReply{EphemeralPub: sealed.EphemeralPub, Ciphertext: sealed.Ciphertext}
	if err := spikeWriteFrame(conn, reply); err != nil {
		log.Warn().Err(err).Msg("zeroca-spike: write reply failed")
		return
	}
	log.Warn().Str("node_id", req.NodeID).Msg("zeroca-spike: sealed cluster secrets to joiner (THROWAWAY)")
}

// --- joiner side -----------------------------------------------------------

// maybeRunSpikeJoiner, when the spike is enabled and a leader addr + invite are
// present, performs the secret pull BEFORE the earliest secret gate: it
// generates a node ECDSA identity, signs the invite transcript, dials the
// leader, opens the sealed secrets, and stages encryption.key + keys/0.key +
// cluster.id on disk while setting opts.ClusterKey in memory. After it returns
// nil, normal boot proceeds as a genesis solo node and finds the staged secrets.
func maybeRunSpikeJoiner(opts *ServeOptions) error {
	if !spikeEnabled() {
		return nil
	}
	leaderAddr := os.Getenv(spikeEnvLeaderAddr)
	token := os.Getenv(spikeEnvInvite)
	if leaderAddr == "" || token == "" {
		return nil // not a joiner
	}
	bundle, err := cluster.DecodeInviteBundle(token)
	if err != nil {
		return fmt.Errorf("zeroca-spike joiner: decode invite: %w", err)
	}
	nodeID := os.Getenv(spikeEnvNodeID)
	if nodeID == "" {
		nodeID = "spike-joiner"
	}

	// 1. node ECDSA P-256 identity + self-signed leaf cert (Path A: cert in req).
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("zeroca-spike joiner: gen key: %w", err)
	}
	certDER, err := spikeSelfSignedCert(priv, nodeID)
	if err != nil {
		return fmt.Errorf("zeroca-spike joiner: self-sign: %w", err)
	}
	leaf, err := x509.ParseCertificate(certDER)
	if err != nil {
		return fmt.Errorf("zeroca-spike joiner: parse own cert: %w", err)
	}
	spki := sha256.Sum256(leaf.RawSubjectPublicKeyInfo)

	// 2. build + sign the canonical transcript (clusterID nil — bootstrap).
	nonce := make([]byte, 16)
	_, _ = rand.Read(nonce)
	tr := encrypt.InviteTranscript{
		ClusterID: nil,
		Nonce:     nonce,
		NodeID:    nodeID,
		Address:   "spike://" + nodeID,
		SPKI:      spki[:],
		Bind:      nil,
	}
	inviteSig := encrypt.SignInviteTranscript(bundle.InvitePriv, tr)
	nodeSig, err := encrypt.SignNodeTranscript(priv, tr)
	if err != nil {
		return fmt.Errorf("zeroca-spike joiner: node sig: %w", err)
	}

	// 3. dial the leader spike listener (retry: leader may still be booting).
	req := spikeJoinRequest{
		NodeID:    nodeID,
		Address:   tr.Address,
		SPKI:      spki[:],
		CertDER:   certDER,
		NodeSig:   nodeSig,
		InviteSig: inviteSig,
		InviteID:  bundle.InviteID,
		Nonce:     nonce,
	}
	sealed, err := spikeDialAndPull(leaderAddr, req)
	if err != nil {
		return fmt.Errorf("zeroca-spike joiner: pull secrets: %w", err)
	}

	// 4. open the sealed secrets with the node identity key.
	aad := append([]byte(nodeID), spki[:]...)
	plain, err := encrypt.OpenFromPeer(priv, encrypt.SealedToPeer{
		EphemeralPub: sealed.EphemeralPub,
		Ciphertext:   sealed.Ciphertext,
	}, []byte(spikeContextInfo), aad)
	if err != nil {
		return fmt.Errorf("zeroca-spike joiner: open secrets: %w", err)
	}
	var secrets spikeSecrets
	if err := json.Unmarshal(plain, &secrets); err != nil {
		return fmt.Errorf("zeroca-spike joiner: unmarshal secrets: %w", err)
	}

	// 5. stage secrets on disk so the normal genesis boot loads them.
	if err := os.MkdirAll(opts.DataDir, 0o755); err != nil {
		return fmt.Errorf("zeroca-spike joiner: mkdir data: %w", err)
	}
	if err := os.WriteFile(filepath.Join(opts.DataDir, "encryption.key"), secrets.EncryptionKey, 0o600); err != nil {
		return fmt.Errorf("zeroca-spike joiner: write encryption.key: %w", err)
	}
	if err := os.WriteFile(filepath.Join(opts.DataDir, nodeconfig.ClusterIDFile), secrets.ClusterID, 0o600); err != nil {
		return fmt.Errorf("zeroca-spike joiner: write cluster.id: %w", err)
	}
	keysDir := nodeconfig.New(opts.DataDir).KEKDir()
	if err := os.MkdirAll(keysDir, 0o700); err != nil {
		return fmt.Errorf("zeroca-spike joiner: mkdir keys: %w", err)
	}
	if err := os.WriteFile(filepath.Join(keysDir, "0.key"), secrets.KEKv0, 0o600); err != nil {
		return fmt.Errorf("zeroca-spike joiner: write keys/0.key: %w", err)
	}

	// 6. set the PSK in memory so the --cluster-key gate passes.
	opts.ClusterKey = secrets.ClusterKey
	log.Warn().Str("node_id", nodeID).Msg("zeroca-spike: joiner staged cluster secrets from invite + SealToPeer (THROWAWAY)")
	return nil
}

func spikeDialAndPull(addr string, req spikeJoinRequest) (spikeSealedReply, error) {
	deadline := time.Now().Add(20 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			lastErr = err
			time.Sleep(200 * time.Millisecond)
			continue
		}
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
		if err := spikeWriteFrame(conn, req); err != nil {
			conn.Close()
			lastErr = err
			time.Sleep(200 * time.Millisecond)
			continue
		}
		var reply spikeSealedReply
		if err := spikeReadFrame(conn, &reply); err != nil {
			conn.Close()
			lastErr = err
			time.Sleep(200 * time.Millisecond)
			continue
		}
		conn.Close()
		if len(reply.Ciphertext) == 0 {
			lastErr = fmt.Errorf("empty sealed reply (leader rejected?)")
			time.Sleep(200 * time.Millisecond)
			continue
		}
		return reply, nil
	}
	return spikeSealedReply{}, fmt.Errorf("dial leader %s: %w", addr, lastErr)
}

func spikeSelfSignedCert(priv *ecdsa.PrivateKey, cn string) ([]byte, error) {
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
	}
	return x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
}

// spikeWriteFrame writes a 4-byte big-endian length prefix then the JSON body.
func spikeWriteFrame(conn net.Conn, v interface{}) error {
	body, err := json.Marshal(v)
	if err != nil {
		return err
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(body)))
	if _, err := conn.Write(hdr[:]); err != nil {
		return err
	}
	_, err = conn.Write(body)
	return err
}

func spikeReadFrame(conn net.Conn, v interface{}) error {
	var hdr [4]byte
	if _, err := readFull(conn, hdr[:]); err != nil {
		return err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n > 1<<20 {
		return fmt.Errorf("spike frame too large: %d", n)
	}
	body := make([]byte, n)
	if _, err := readFull(conn, body); err != nil {
		return err
	}
	return json.Unmarshal(body, v)
}

func readFull(conn net.Conn, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := conn.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}
