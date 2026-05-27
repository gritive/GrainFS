package serveruntime

// THROWAWAY feasibility spike (Zero-CA network-path de-risk). Every line in
// this file is dead unless GRAINFS_ZEROCA_SPIKE=1. It exists ONLY to prove that
// a node holding NO cluster secrets can boot to a running server using an invite
// + the encrypt.SealToPeer primitive, AND that a brand-new joiner whose SPKI is
// in NObody's accept-set can reach a handler over the real cluster transport
// (QUIC). NOT production code; do not build on it.
//
// Wire transport here is QUIC (the actual cluster transport), framed with
// length-prefixed binary fields — NOT JSON, NOT TCP. The earlier TCP+JSON draft
// cheated on both the transport and the framing; this version de-risks the real
// question: can an unknown-SPKI peer complete a QUIC handshake + stream to a
// handler? It can — but ONLY via a DEDICATED spike listener that bypasses the
// two production SPKI gates (see below). The normal psk/mux listeners keep both
// gates unchanged.
//
// Production gates this spike listener DELIBERATELY bypasses (fresh-read line
// numbers in internal/transport/quic.go, 2026-05-27):
//   - buildServerTLSConfig (~:1445) sets VerifyPeerCertificate: pinAcceptedSPKI
//     (~:518) — TLS-layer accept-set pin. The spike listener uses a permissive
//     VerifyPeerCertificate that CAPTURES but does NOT pin the peer SPKI.
//   - acceptLoop (~:448) does an EXPLICIT post-accept SPKI check (~:465, comment
//     ~:454 "quic-go does NOT reliably enforce ClientAuth") that closes the conn
//     with "peer cert rejected" BEFORE ALPN routing. The spike listener runs its
//     OWN minimal accept loop with NO accept-set rejection.
// The spike listener NEVER touches the identity store, so the joiner's SPKI is
// never added to any accept-set yet the QUIC handshake+stream succeeds — that is
// the bypass this spike proves.
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
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"time"

	"github.com/quic-go/quic-go"
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
	spikeALPN          = "grainfs-zeroca-spike" // dedicated, isolated from prod psk/mux ALPNs
	spikeMaxFrame      = 1 << 20
)

func spikeEnabled() bool { return os.Getenv(spikeEnvOn) == "1" }

// --- binary wire framing (NO JSON) ----------------------------------------
//
// Every message is a sequence of length-prefixed []byte fields: each field is a
// big-endian uint32 length followed by that many bytes. Fixed field order per
// message type (see encode/decode helpers). This is the throwaway-spike framing
// standing in for the production FlatBuffers-over-QUIC wire.

func spikePutField(buf []byte, f []byte) []byte {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(f)))
	buf = append(buf, hdr[:]...)
	return append(buf, f...)
}

// spikeReadFields reads exactly n length-prefixed []byte fields from r.
func spikeReadFields(r io.Reader, n int) ([][]byte, error) {
	out := make([][]byte, n)
	for i := 0; i < n; i++ {
		var hdr [4]byte
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return nil, err
		}
		sz := binary.BigEndian.Uint32(hdr[:])
		if sz > spikeMaxFrame {
			return nil, fmt.Errorf("spike field too large: %d", sz)
		}
		body := make([]byte, sz)
		if _, err := io.ReadFull(r, body); err != nil {
			return nil, err
		}
		out[i] = body
	}
	return out, nil
}

// spikeJoinRequest is the joiner -> leader request, encoded as 8 binary fields.
type spikeJoinRequest struct {
	NodeID    string
	Address   string
	SPKI      []byte
	CertDER   []byte
	NodeSig   []byte
	InviteSig []byte
	InviteID  string
	Nonce     []byte
}

func (q spikeJoinRequest) encode() []byte {
	var buf []byte
	buf = spikePutField(buf, []byte(q.NodeID))
	buf = spikePutField(buf, []byte(q.Address))
	buf = spikePutField(buf, q.SPKI)
	buf = spikePutField(buf, q.CertDER)
	buf = spikePutField(buf, q.NodeSig)
	buf = spikePutField(buf, q.InviteSig)
	buf = spikePutField(buf, []byte(q.InviteID))
	buf = spikePutField(buf, q.Nonce)
	return buf
}

func decodeSpikeJoinRequest(r io.Reader) (spikeJoinRequest, error) {
	f, err := spikeReadFields(r, 8)
	if err != nil {
		return spikeJoinRequest{}, err
	}
	return spikeJoinRequest{
		NodeID:    string(f[0]),
		Address:   string(f[1]),
		SPKI:      f[2],
		CertDER:   f[3],
		NodeSig:   f[4],
		InviteSig: f[5],
		InviteID:  string(f[6]),
		Nonce:     f[7],
	}, nil
}

// spikeSecrets is the cleartext payload sealed to the joiner's pubkey, encoded
// as 4 binary fields (NO JSON inside the sealed blob either).
type spikeSecrets struct {
	EncryptionKey []byte
	KEKv0         []byte
	ClusterID     []byte
	ClusterKey    string
}

func (s spikeSecrets) encode() []byte {
	var buf []byte
	buf = spikePutField(buf, s.EncryptionKey)
	buf = spikePutField(buf, s.KEKv0)
	buf = spikePutField(buf, s.ClusterID)
	buf = spikePutField(buf, []byte(s.ClusterKey))
	return buf
}

func decodeSpikeSecrets(b []byte) (spikeSecrets, error) {
	f, err := spikeReadFields(newByteReader(b), 4)
	if err != nil {
		return spikeSecrets{}, err
	}
	return spikeSecrets{
		EncryptionKey: f[0],
		KEKv0:         f[1],
		ClusterID:     f[2],
		ClusterKey:    string(f[3]),
	}, nil
}

// spikeSealedReply wraps a SealToPeer blob: 2 binary fields.
func encodeSpikeSealedReply(s encrypt.SealedToPeer) []byte {
	var buf []byte
	buf = spikePutField(buf, s.EphemeralPub)
	buf = spikePutField(buf, s.Ciphertext)
	return buf
}

func decodeSpikeSealedReply(r io.Reader) (encrypt.SealedToPeer, error) {
	f, err := spikeReadFields(r, 2)
	if err != nil {
		return encrypt.SealedToPeer{}, err
	}
	return encrypt.SealedToPeer{EphemeralPub: f[0], Ciphertext: f[1]}, nil
}

type byteReader struct {
	b []byte
	i int
}

func newByteReader(b []byte) *byteReader { return &byteReader{b: b} }

func (r *byteReader) Read(p []byte) (int, error) {
	if r.i >= len(r.b) {
		return 0, io.EOF
	}
	n := copy(p, r.b[r.i:])
	r.i += n
	return n, nil
}

// --- leader side ----------------------------------------------------------

// maybeStartSpikeLeader, when the spike is enabled and a listen addr is set,
// mints an Ed25519 invite, writes the bundle token to <dataDir>/spike-invite.token
// for the test/operator to hand to the joiner, and starts a DEDICATED QUIC
// listener (its own ephemeral self-signed cert, permissive peer verification,
// own accept loop with NO accept-set gate, single spike ALPN) that serves the
// secret-delivery RPC. It returns immediately; the listener runs in the
// background until process exit.
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

	tlsConf, err := spikeLeaderTLSConfig()
	if err != nil {
		log.Error().Err(err).Msg("zeroca-spike: build leader TLS failed")
		return
	}
	ln, err := quic.ListenAddr(addr, tlsConf, defaultSpikeQUICConfig())
	if err != nil {
		log.Error().Err(err).Str("addr", addr).Msg("zeroca-spike: QUIC listen failed")
		return
	}
	log.Warn().Str("addr", addr).Str("invite_id", inviteID).Msg("zeroca-spike: leader QUIC secret-delivery listener up (THROWAWAY)")

	// Dedicated minimal accept loop: NO production acceptLoop SPKI gate. Routes
	// every accepted conn straight to the seal handler.
	go func() {
		for {
			conn, err := ln.Accept(context.Background())
			if err != nil {
				return // listener closed
			}
			go spikeHandleConn(conn, opts, invitePub, inviteID)
		}
	}()
}

// spikeLeaderTLSConfig returns the listener TLS config that BYPASSES the
// production accept-set pin: an ephemeral self-signed P-256 cert + a permissive
// VerifyPeerCertificate that CAPTURES the peer SPKI (logging it) but pins
// NOTHING. It never consults any IdentitySnapshot / accept-set.
func spikeLeaderTLSConfig() (*tls.Config, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("gen leader spike key: %w", err)
	}
	certDER, err := spikeSelfSignedCert(priv, "zeroca-spike-leader")
	if err != nil {
		return nil, fmt.Errorf("self-sign leader spike cert: %w", err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{certDER}, PrivateKey: priv}},
		ClientAuth:   tls.RequireAnyClientCert,
		NextProtos:   []string{spikeALPN},
		// quic-go requires InsecureSkipVerify when a custom VerifyPeerCertificate
		// is used; the spike VerifyPeerCertificate captures but does NOT pin.
		InsecureSkipVerify: true,
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			if len(rawCerts) == 0 {
				return errors.New("zeroca-spike: no peer cert")
			}
			cert, err := x509.ParseCertificate(rawCerts[0])
			if err != nil {
				return fmt.Errorf("zeroca-spike: parse peer cert: %w", err)
			}
			spki := sha256.Sum256(cert.RawSubjectPublicKeyInfo)
			// THE BYPASS: log the unknown SPKI and accept it. No accept-set lookup.
			log.Warn().Str("peer_spki", hex.EncodeToString(spki[:])).
				Msg("zeroca-spike: accepted unknown-SPKI peer at TLS layer (NO accept-set check) (THROWAWAY)")
			return nil
		},
	}, nil
}

func defaultSpikeQUICConfig() *quic.Config {
	return &quic.Config{
		KeepAlivePeriod: 5 * time.Second,
		MaxIdleTimeout:  30 * time.Second,
	}
}

func spikeHandleConn(conn *quic.Conn, opts ServeOptions, invitePub []byte, inviteID string) {
	defer func() { _ = conn.CloseWithError(0, "spike done") }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Confirm we actually reached the handler with an unknown-SPKI peer: the
	// post-accept SPKI gate of the production acceptLoop was NOT run here.
	state := conn.ConnectionState()
	if len(state.TLS.PeerCertificates) > 0 {
		peerSPKI := sha256.Sum256(state.TLS.PeerCertificates[0].RawSubjectPublicKeyInfo)
		log.Warn().Str("peer_spki", hex.EncodeToString(peerSPKI[:])).
			Msg("zeroca-spike: handler reached by unknown-SPKI peer over QUIC (bypassed acceptLoop gate) (THROWAWAY)")
	}

	stream, err := conn.AcceptStream(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("zeroca-spike: accept stream failed")
		return
	}
	defer func() { _ = stream.Close() }()

	req, err := decodeSpikeJoinRequest(stream)
	if err != nil {
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
	// canonical transcript. clusterID is nil on BOTH sides here, since clusterID
	// delivery is the very thing being bootstrapped (documented KNOWN GAP above).
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
	aad := append([]byte(req.NodeID), req.SPKI...)
	sealed, err := encrypt.SealToPeer(ecPub, secrets.encode(), []byte(spikeContextInfo), aad)
	if err != nil {
		log.Warn().Err(err).Msg("zeroca-spike: SealToPeer failed")
		return
	}
	if _, err := stream.Write(encodeSpikeSealedReply(sealed)); err != nil {
		log.Warn().Err(err).Msg("zeroca-spike: write reply failed")
		return
	}
	// stream.Close (deferred) closes the send side cleanly so the joiner reads to EOF.
	log.Warn().Str("node_id", req.NodeID).Msg("zeroca-spike: sealed cluster secrets to joiner over QUIC (THROWAWAY)")
}

// --- joiner side -----------------------------------------------------------

// maybeRunSpikeJoiner, when the spike is enabled and a leader addr + invite are
// present, performs the secret pull BEFORE the earliest secret gate: it
// generates a node ECDSA identity, signs the invite transcript, dials the
// leader over QUIC presenting that node cert (unknown SPKI), opens the sealed
// secrets, and stages encryption.key + keys/0.key + cluster.id on disk while
// setting opts.ClusterKey in memory. After it returns nil, normal boot proceeds
// as a genesis solo node and finds the staged secrets.
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

	// 3. dial the leader spike QUIC listener (retry: leader may still be booting).
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
	sealed, err := spikeDialAndPull(leaderAddr, priv, certDER, req)
	if err != nil {
		return fmt.Errorf("zeroca-spike joiner: pull secrets: %w", err)
	}

	// 4. open the sealed secrets with the node identity key.
	aad := append([]byte(nodeID), spki[:]...)
	plain, err := encrypt.OpenFromPeer(priv, sealed, []byte(spikeContextInfo), aad)
	if err != nil {
		return fmt.Errorf("zeroca-spike joiner: open secrets: %w", err)
	}
	secrets, err := decodeSpikeSecrets(plain)
	if err != nil {
		return fmt.Errorf("zeroca-spike joiner: decode secrets: %w", err)
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
	log.Warn().Str("node_id", nodeID).Msg("zeroca-spike: joiner staged cluster secrets from invite + SealToPeer over QUIC (THROWAWAY)")
	return nil
}

// spikeDialAndPull dials the leader spike QUIC listener presenting the joiner's
// (unknown-SPKI) node cert, sends the join request on a stream, and reads back
// the sealed reply. It retries until a deadline since the leader may still be
// booting.
func spikeDialAndPull(addr string, priv *ecdsa.PrivateKey, certDER []byte, req spikeJoinRequest) (encrypt.SealedToPeer, error) {
	clientTLS := &tls.Config{
		Certificates:       []tls.Certificate{{Certificate: [][]byte{certDER}, PrivateKey: priv}},
		InsecureSkipVerify: true, // spike leader cert is ephemeral self-signed
		NextProtos:         []string{spikeALPN},
	}
	deadline := time.Now().Add(20 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		reply, err := spikeDialOnce(addr, clientTLS, req)
		if err != nil {
			lastErr = err
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if len(reply.Ciphertext) == 0 {
			lastErr = fmt.Errorf("empty sealed reply (leader rejected?)")
			time.Sleep(200 * time.Millisecond)
			continue
		}
		return reply, nil
	}
	return encrypt.SealedToPeer{}, fmt.Errorf("dial leader %s: %w", addr, lastErr)
}

func spikeDialOnce(addr string, clientTLS *tls.Config, req spikeJoinRequest) (encrypt.SealedToPeer, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := quic.DialAddr(ctx, addr, clientTLS, defaultSpikeQUICConfig())
	if err != nil {
		return encrypt.SealedToPeer{}, err
	}
	defer func() { _ = conn.CloseWithError(0, "spike done") }()

	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return encrypt.SealedToPeer{}, err
	}
	if _, err := stream.Write(req.encode()); err != nil {
		return encrypt.SealedToPeer{}, err
	}
	// Close the send side so the leader reads our request to EOF; then read the
	// reply to EOF (leader closes its send side after writing).
	_ = stream.Close()
	return decodeSpikeSealedReply(stream)
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
