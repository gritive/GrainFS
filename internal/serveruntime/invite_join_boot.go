package serveruntime

// Zero-CA invite-join boot orchestration (W9b, JOINER side).
//
// A brand-new node holding NO cluster secrets boots into a running voter using
// only an InviteBundle (W1) over the dedicated QUIC join transport (W4). This
// productionizes the throwaway de-risk spike (commit 48c7bdb's zero_ca_spike.go)
// into the real two-phase flow against W7's server handler:
//
//	Phase-1 (pre-boot, maybeInviteJoin):
//	  - resolve a STABLE node id (write back to opts.NodeID),
//	  - require a stable advertised raft addr,
//	  - generate the node ECDSA identity + self-signed leaf, persist UNWRAPPED at
//	    keys.d/node.key.unsealed (0600) BEFORE redeeming,
//	  - sign the canonical InviteTranscript (InviteSig=ed25519 invite key,
//	    NodeSig=ecdsa node key),
//	  - DialJoin(SeedAddr, SeedSPKI, nodeCert) and send JoinRequest{JoinPhase:1},
//	  - OpenFromPeer the SealedBootstrap with the bindCtx W7 used
//	    (clusterID‖inviteID‖nodeID‖leaderID), stage every secret on disk
//	    (encryption.key, keys/<gen>.key, cluster.id), SealNodeKey under the
//	    cluster KEK gen-0 to keys.d/node.key.enc, shred node.key.unsealed,
//	  - set opts.ClusterKey in memory (transport PSK) so bootValidateConfig
//	    passes, and persist a .invite-join-pending sentinel so a crash before
//	    Phase-2 resumes the ACK.
//
//	Phase-2 (post-boot, bootInviteJoinPhase2 from bootWALAndForwarders):
//	  - LoadNodeKey (assert SPKI == pending), DialJoin again, send
//	    JoinRequest{JoinPhase:2} to finalize raft membership, clear the sentinel.
//
// On restart with Phase-1 artifacts durable but not ACKed → the resume gate
// returns Resume and the post-boot path re-sends Phase-2 (idempotent on W7).
//
// Wire is length-prefixed binary (transport.JoinPutField/JoinReadFields) over
// the join ALPN — NO JSON, NO TCP. The transcript ClusterID is the raw 16-byte
// cluster.id from the bundle (hex-decoded), matching W7's gateInvite which binds
// r.clusterID into the transcript (closes the spike's documented nil-clusterID
// gap).

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/nodeconfig"
	"github.com/gritive/GrainFS/internal/transport"
)

// inviteBundleEnv is the env var carrying the operator-handed invite token (W1
// EncodeInviteBundle output). Secrets are passed by env/file only (project rule).
const inviteBundleEnv = "GRAINFS_INVITE_BUNDLE"

// invitePendingFile is the Zero-CA invite-join resume sentinel. Present from
// Phase-1 staging until the Phase-2 ACK succeeds. Distinct from the legacy
// .join-pending sentinel (KEK-handshake join). Its presence is the "not acked"
// signal of the resume gate.
const invitePendingFile = ".invite-join-pending"

// nodeKeyUnsealedFile is the UNWRAPPED node key persisted before redeeming the
// invite, so a crash mid-Phase-1 does not lose the identity the leader already
// bound the invite to. It is shredded after SealNodeKey writes node.key.enc.
const nodeKeyUnsealedFile = "node.key.unsealed"

// inviteJoinDialTimeout bounds a single Phase-1/Phase-2 dial+exchange.
const inviteJoinDialTimeout = 30 * time.Second

// inviteJoinState is the Phase-1 outcome threaded onto bootState (via Config) so
// the post-boot Phase-2 ACK can redrive DialJoin without re-reading the bundle.
type inviteJoinState struct {
	seedAddr  string
	seedSPKI  [32]byte
	inviteID  string
	nodeID    string
	raftAddr  string
	clusterID []byte   // raw 16 bytes
	leaderID  string   // from Phase-1 reply; component of the seal bindCtx
	nodeSPKI  [32]byte // joiner's own node identity SPKI (asserted at Phase-2)
}

// inviteJoinDecision is the resume gate's classification.
type inviteJoinDecision int

const (
	// inviteNormalBoot: no invite bundle, or all durable + acked → ordinary boot.
	inviteNormalBoot inviteJoinDecision = iota
	// inviteFreshJoin: bundle present, no persisted invite node key → run Phase-1.
	inviteFreshJoin
	// inviteResume: bundle present + persisted node key + incomplete (artifacts
	// missing OR not acked) → skip Phase-1 pull, resume Phase-2 ACK.
	inviteResume
)

// classifyInviteJoinResume is the PURE resume gate. Inputs:
//
//   - bundlePresent:         an invite bundle token was supplied (env var set).
//   - persistedInviteNodeKey: keys.d/node.key.unsealed OR keys.d/node.key.enc
//     exists (the joiner already generated + persisted its identity in Phase-1).
//   - artifactsComplete:     encryption.key + cluster.id + keys/0.key +
//     keys.d/node.key.enc all present AND node.key.unsealed absent (Phase-1
//     staging + seal fully landed).
//   - acked:                 the .invite-join-pending sentinel is ABSENT (Phase-2
//     membership ACK completed).
//
// Classification table:
//
//	bundle && !persistedKey                  → FreshJoin
//	bundle && persistedKey && !(complete&&acked) → Resume
//	otherwise (incl. !bundle, or all durable)    → NormalBoot
func classifyInviteJoinResume(bundlePresent, persistedInviteNodeKey, artifactsComplete, acked bool) inviteJoinDecision {
	if !bundlePresent {
		return inviteNormalBoot
	}
	if !persistedInviteNodeKey {
		return inviteFreshJoin
	}
	if artifactsComplete && acked {
		return inviteNormalBoot
	}
	return inviteResume
}

// inviteJoinPaths bundles the on-disk locations the gate + staging touch.
type inviteJoinPaths struct {
	encryptionKey   string
	clusterID       string
	kekGen0         string
	nodeKeyEnc      string
	nodeKeyUnsealed string
	pendingSentinel string
}

func inviteJoinPathsFor(dataDir string) inviteJoinPaths {
	keysDir := nodeconfig.New(dataDir).KEKDir()
	keysD := filepath.Join(dataDir, "keys.d")
	return inviteJoinPaths{
		encryptionKey:   filepath.Join(dataDir, "encryption.key"),
		clusterID:       filepath.Join(dataDir, nodeconfig.ClusterIDFile),
		kekGen0:         filepath.Join(keysDir, "0.key"),
		nodeKeyEnc:      filepath.Join(keysD, "node.key.enc"),
		nodeKeyUnsealed: filepath.Join(keysD, nodeKeyUnsealedFile),
		pendingSentinel: filepath.Join(dataDir, invitePendingFile),
	}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// gateInviteJoin reads disk to classify the resume decision for dataDir.
func gateInviteJoin(dataDir string, bundlePresent bool) inviteJoinDecision {
	p := inviteJoinPathsFor(dataDir)
	persistedKey := fileExists(p.nodeKeyUnsealed) || fileExists(p.nodeKeyEnc)
	artifactsComplete := fileExists(p.encryptionKey) &&
		fileExists(p.clusterID) &&
		fileExists(p.kekGen0) &&
		fileExists(p.nodeKeyEnc) &&
		!fileExists(p.nodeKeyUnsealed)
	acked := !fileExists(p.pendingSentinel)
	return classifyInviteJoinResume(bundlePresent, persistedKey, artifactsComplete, acked)
}

// maybeInviteJoin is the top-of-RunFromOptions hook. It returns a non-nil
// *inviteJoinState ONLY when this boot is a Zero-CA invite-join (FreshJoin or
// Resume); the caller threads it onto Config so boot enters inviteJoinMode. On
// NormalBoot it returns (nil, nil) and is a complete no-op.
func maybeInviteJoin(ctx context.Context, opts *ServeOptions) (*inviteJoinState, error) {
	token := os.Getenv(inviteBundleEnv)
	decision := gateInviteJoin(opts.DataDir, token != "")
	if decision == inviteNormalBoot {
		return nil, nil
	}

	bundle, err := cluster.DecodeInviteBundle(token)
	if err != nil {
		return nil, fmt.Errorf("decode invite bundle: %w", err)
	}
	clusterID, err := hex.DecodeString(bundle.ClusterIDHex)
	if err != nil {
		return nil, fmt.Errorf("decode cluster id hex: %w", err)
	}

	// Resolve a STABLE node id BEFORE Phase-1 and write it back so the later
	// normal boot (bootValidateConfig) resolves the IDENTICAL id. GenerateNodeID
	// persists <dataDir>/node-id and is idempotent.
	if opts.NodeID == "" {
		id, err := GenerateNodeID(opts.DataDir)
		if err != nil {
			return nil, fmt.Errorf("resolve node id: %w", err)
		}
		opts.NodeID = id
	}

	// Require a stable advertised raft addr: the leader records this as the
	// joiner's membership address, so an ephemeral 127.0.0.1:0 (resolved only
	// after the local listener binds) would advertise a port the cluster can
	// never dial back.
	raftAddr := strings.TrimSpace(opts.RaftAddr)
	if raftAddr == "" || strings.HasSuffix(raftAddr, ":0") {
		return nil, fmt.Errorf("invite-join requires a stable --raft-addr (got %q); an ephemeral :0 port cannot be advertised to the cluster", opts.RaftAddr)
	}

	st := &inviteJoinState{
		seedAddr:  bundle.SeedAddr,
		seedSPKI:  bundle.SeedSPKI,
		inviteID:  bundle.InviteID,
		nodeID:    opts.NodeID,
		raftAddr:  raftAddr,
		clusterID: clusterID,
	}

	if decision == inviteResume {
		// Phase-1 already staged secrets + identity on a prior boot. The
		// --cluster-key gate (bootValidateConfig) runs BEFORE bootQUICTransport's
		// ResolveClusterKey, so we MUST populate opts.ClusterKey here from the
		// transport PSK that Phase-1 mirrored to keys.d/current.key — leaving it
		// empty would trip ErrEmptyClusterKey before disk is ever read. The node
		// SPKI + leaderID are reloaded from node.key.enc + the sentinel at Phase-2.
		psk, err := transport.NewKeystore(opts.DataDir).ReadCurrent()
		if err != nil || psk == "" {
			return nil, fmt.Errorf("invite-join resume: transport PSK (keys.d/current.key) missing or unreadable: %w", err)
		}
		opts.ClusterKey = psk
		log.Warn().Str("node_id", st.nodeID).Msg("zero-CA invite-join: resuming (Phase-1 artifacts durable, Phase-2 ACK pending)")
		return st, nil
	}

	// FreshJoin: run Phase-1 (pull + stage + seal + sentinel).
	if err := inviteJoinPhase1(ctx, opts, bundle, st); err != nil {
		return nil, err
	}
	return st, nil
}

// inviteJoinPhase1 performs the secret pull + on-disk staging. It mutates st
// (leaderID, nodeSPKI) and opts (ClusterKey).
func inviteJoinPhase1(ctx context.Context, opts *ServeOptions, bundle cluster.InviteBundle, st *inviteJoinState) error {
	paths := inviteJoinPathsFor(opts.DataDir)
	keysD := filepath.Join(opts.DataDir, "keys.d")

	// 1. node ECDSA P-256 identity + self-signed leaf (Path A: cert in request).
	// clusterIDHex is the cert SAN host (GenerateNodeIdentity takes a string).
	cert, spki, err := transport.GenerateNodeIdentity(bundle.ClusterIDHex, st.nodeID)
	if err != nil {
		return fmt.Errorf("generate node identity: %w", err)
	}
	priv, ok := cert.PrivateKey.(*ecdsa.PrivateKey)
	if !ok {
		return fmt.Errorf("node identity key is %T, want *ecdsa.PrivateKey", cert.PrivateKey)
	}
	certDER := cert.Certificate[0]
	st.nodeSPKI = spki

	// Persist the UNWRAPPED node key BEFORE redeeming. If we crash after the
	// leader bound the invite to this SPKI but before we can seal the key under
	// the (not-yet-staged) KEK, the unsealed key lets resume reuse the identity.
	if err := writeNodeKeyUnsealed(keysD, paths.nodeKeyUnsealed, priv); err != nil {
		return err
	}

	// 2. write the resume sentinel BEFORE the dial: from here on a crash must
	// resume Phase-2 rather than restart Phase-1 with a fresh identity.
	if err := writeInvitePendingSentinel(opts.DataDir, paths.pendingSentinel, st); err != nil {
		return err
	}

	// 3. build + sign the canonical transcript. ClusterID is the raw 16-byte
	// cluster.id (matches W7 gateInvite). Nonce is joiner-generated.
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return fmt.Errorf("invite nonce: %w", err)
	}
	tr := encrypt.InviteTranscript{
		ClusterID: st.clusterID,
		Nonce:     nonce,
		NodeID:    st.nodeID,
		Address:   st.raftAddr,
		SPKI:      spki[:],
		Bind:      nil,
	}
	inviteSig := encrypt.SignInviteTranscript(bundle.InvitePriv, tr)
	nodeSig, err := encrypt.SignNodeTranscript(priv, tr)
	if err != nil {
		return fmt.Errorf("sign node transcript: %w", err)
	}

	// 4. advertise this node's OWN join listener (W9a) so a non-leader seed can
	// later redirect us to the leader (redirect itself is a follow-up task).
	_, joinListenerSPKI, err := LoadOrCreateJoinListenerCert(opts.DataDir)
	if err != nil {
		return fmt.Errorf("load join listener cert: %w", err)
	}
	joinListenAddr := resolveJoinListenAddr(opts.JoinListenAddr, st.raftAddr)

	req := cluster.JoinRequest{
		JoinPhase:              1,
		NodeID:                 st.nodeID,
		Address:                st.raftAddr,
		SPKI:                   spki[:],
		CertDER:                certDER,
		NodeSig:                nodeSig,
		InviteSig:              inviteSig,
		InviteID:               bundle.InviteID,
		HandshakeNonce:         nonce,
		JoinerJoinListenerAddr: joinListenAddr,
		JoinerJoinListenerSPKI: joinListenerSPKI[:],
	}

	// 5. dial the seed join listener (Phase-1), pinning its SPKI.
	reply, err := inviteJoinDial(ctx, bundle.SeedAddr, bundle.SeedSPKI, cert, req)
	if err != nil {
		return fmt.Errorf("phase-1 dial: %w", err)
	}
	if !reply.Accepted {
		return fmt.Errorf("phase-1 rejected: status=%s msg=%s", reply.Status, reply.Message)
	}
	if len(reply.SealedBootstrap) == 0 {
		return fmt.Errorf("phase-1 reply missing sealed bootstrap")
	}
	st.leaderID = reply.LeaderID

	// 6. open the sealed bootstrap with the node key. bindCtx/aad MUST match W7's
	// sealBindContext exactly: clusterID‖inviteID‖nodeID‖leaderID.
	ephPub, ciphertext, err := cluster.DecodeSealedBootstrap(reply.SealedBootstrap)
	if err != nil {
		return fmt.Errorf("decode sealed bootstrap: %w", err)
	}
	bindCtx := inviteSealBindContext(st.clusterID, bundle.InviteID, st.nodeID, reply.LeaderID)
	plain, err := encrypt.OpenFromPeer(priv, encrypt.SealedToPeer{EphemeralPub: ephPub, Ciphertext: ciphertext}, bindCtx, bindCtx)
	if err != nil {
		return fmt.Errorf("open sealed bootstrap: %w", err)
	}
	encKey, kekGens, psk, err := cluster.DecodeBootstrapSecretsPayload(plain)
	if err != nil {
		return fmt.Errorf("decode bootstrap secrets: %w", err)
	}

	// 7. stage every secret on disk where the normal boot expects them.
	if err := stageInviteSecrets(opts.DataDir, encKey, kekGens, st.clusterID); err != nil {
		return err
	}

	// 8. SealNodeKey under the cluster KEK gen-0 (the active KEK on a non-genesis
	// boot, == the staged keys/0.key bytes), then shred the unsealed key.
	kekGen0 := kekForGen(kekGens, 0)
	if len(kekGen0) == 0 {
		return fmt.Errorf("bootstrap secrets missing KEK gen-0")
	}
	if err := transport.SealNodeKey(opts.DataDir, kekGen0, cert); err != nil {
		return fmt.Errorf("seal node key: %w", err)
	}
	shredFile(paths.nodeKeyUnsealed)

	// 9. set the transport PSK in memory so bootValidateConfig's --cluster-key
	// gate passes (PSK is the hex cluster-key string, applied verbatim). Also
	// mirror it to keys.d/current.key NOW so a crash before bootQUICTransport
	// still lets the resume boot (which leaves opts.ClusterKey empty) read the
	// PSK from disk (ResolveClusterKey: disk wins).
	opts.ClusterKey = string(psk)
	if len(psk) > 0 {
		if err := transport.NewKeystore(opts.DataDir).WriteCurrent(string(psk)); err != nil {
			return fmt.Errorf("mirror transport PSK to keystore: %w", err)
		}
	}

	log.Warn().
		Str("node_id", st.nodeID).
		Str("leader_id", reply.LeaderID).
		Str("seed", bundle.SeedAddr).
		Msg("zero-CA invite-join: Phase-1 complete (secrets staged, identity sealed); Phase-2 ACK pending")
	return nil
}

// bootInviteJoinPhase2 finalizes membership AFTER the meta-raft transport + raft
// are up (called from bootWALAndForwarders, before WaitDEKReady). It reloads the
// node key (asserting the SPKI matches the Phase-1 identity), dials the seed join
// listener with JoinPhase:2, and clears the resume sentinel on success.
func bootInviteJoinPhase2(ctx context.Context, state *bootState) error {
	st := state.inviteJoin
	if st == nil {
		return fmt.Errorf("invite-join Phase-2: nil state")
	}

	// The active cluster KEK gen-0 is the KEK node.key.enc was sealed under.
	if state.kekStore == nil {
		return fmt.Errorf("invite-join Phase-2: KEK store not wired")
	}
	kekGen0, err := state.kekStore.Get(0)
	if err != nil {
		return fmt.Errorf("invite-join Phase-2: get KEK gen-0: %w", err)
	}
	cert, spki, err := transport.LoadNodeKey(state.cfg.DataDir, kekGen0)
	if err != nil {
		return fmt.Errorf("invite-join Phase-2: load node key: %w", err)
	}
	// On a fresh-join boot st.nodeSPKI is set from Phase-1; on resume it is zero
	// (Phase-1 ran in a prior process) — skip the equality assert in that case.
	if st.nodeSPKI != ([32]byte{}) && spki != st.nodeSPKI {
		return fmt.Errorf("invite-join Phase-2: loaded node SPKI does not match Phase-1 identity")
	}
	st.nodeSPKI = spki

	// Resolve leaderID: prefer the Phase-1 value, fall back to the persisted
	// sentinel (resume across process restart).
	if st.leaderID == "" {
		if persisted, ok := readInvitePendingSentinel(state.cfg.DataDir); ok {
			st.leaderID = persisted.leaderID
			if st.inviteID == "" {
				st.inviteID = persisted.inviteID
			}
		}
	}

	req := cluster.JoinRequest{
		JoinPhase: 2,
		NodeID:    st.nodeID,
		Address:   st.raftAddr,
		SPKI:      spki[:],
		InviteID:  st.inviteID,
	}
	reply, err := inviteJoinDial(ctx, st.seedAddr, st.seedSPKI, cert, req)
	if err != nil {
		return fmt.Errorf("invite-join Phase-2 dial: %w", err)
	}
	if !reply.Accepted {
		return fmt.Errorf("invite-join Phase-2 rejected: status=%s msg=%s", reply.Status, reply.Message)
	}

	// reply.PeerSPKIs (the cluster per-node accept-set) is intentionally NOT
	// installed here: the joiner's normal cluster QUIC transport derives its
	// accept-set from the shared transport PSK (NewQUICTransport(transportPSK) →
	// DeriveClusterIdentity), which Phase-1 already delivered. The leader dials
	// the freshly-joined node using that SAME PSK-derived cluster identity, so
	// AppendEntries catch-up (and thus the gen-0 DEK Apply the WaitDEKReady gate
	// waits on) succeeds without per-node SPKIs. Wiring reply.PeerSPKIs into a
	// registry∪PSK accept-set UNION is the deferred work tracked by the
	// SetOnPeersChanged TODO in boot_phases_raft.go.

	// Membership finalized: clear the resume barrier.
	_ = os.Remove(filepath.Join(state.cfg.DataDir, invitePendingFile))
	log.Info().
		Str("node_id", st.nodeID).
		Str("leader_id", st.leaderID).
		Msg("zero-CA invite-join: Phase-2 ACK complete — node is a cluster member")
	return nil
}

// inviteJoinDial dials a JoinListener at addr (pinning serverSPKI), sends the
// framed JoinRequest, and reads the framed JoinReply. One field in each
// direction, length-prefixed binary (NO JSON) — mirrors W7 HandleJoinStream.
func inviteJoinDial(ctx context.Context, addr string, serverSPKI [32]byte, clientCert tls.Certificate, req cluster.JoinRequest) (cluster.JoinReply, error) {
	dialCtx, cancel := context.WithTimeout(ctx, inviteJoinDialTimeout)
	defer cancel()
	stream, closeConn, err := transport.DialJoin(dialCtx, addr, serverSPKI, clientCert)
	if err != nil {
		return cluster.JoinReply{}, err
	}
	defer func() { _ = closeConn() }()

	reqBlob, err := cluster.EncodeJoinRequest(req)
	if err != nil {
		return cluster.JoinReply{}, fmt.Errorf("encode join request: %w", err)
	}
	if _, err := stream.Write(transport.JoinPutField(nil, reqBlob)); err != nil {
		return cluster.JoinReply{}, fmt.Errorf("write join request: %w", err)
	}
	// Close the send side so the leader reads our request to EOF.
	_ = stream.Close()

	fields, err := transport.JoinReadFields(stream, 1)
	if err != nil {
		return cluster.JoinReply{}, fmt.Errorf("read join reply: %w", err)
	}
	reply, err := cluster.DecodeJoinReply(fields[0])
	if err != nil {
		return cluster.JoinReply{}, fmt.Errorf("decode join reply: %w", err)
	}
	return *reply, nil
}

// inviteSealBindContext reproduces W7 MetaJoinReceiver.sealBindContext:
// clusterID‖inviteID‖nodeID‖leaderID. The joiner derives the SAME bytes to open
// the sealed bootstrap (both contextInfo and aad use this value).
func inviteSealBindContext(clusterID []byte, inviteID, nodeID, leaderID string) []byte {
	out := make([]byte, 0, len(clusterID)+len(inviteID)+len(nodeID)+len(leaderID))
	out = append(out, clusterID...)
	out = append(out, inviteID...)
	out = append(out, nodeID...)
	out = append(out, leaderID...)
	return out
}

// kekForGen returns the key bytes for generation gen, or nil if absent.
func kekForGen(gens []cluster.KEKGen, gen uint32) []byte {
	for _, g := range gens {
		if g.Gen == gen {
			return g.Key
		}
	}
	return nil
}

// stageInviteSecrets writes encryption.key, every keys/<gen>.key, and cluster.id
// (raw 16 bytes) where the normal boot (LoadOrCreateEncryptionKey, wireDEKKeeper)
// expects them.
func stageInviteSecrets(dataDir string, encKey []byte, kekGens []cluster.KEKGen, clusterID []byte) error {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return fmt.Errorf("stage secrets: mkdir data: %w", err)
	}
	if len(encKey) > 0 {
		if err := os.WriteFile(filepath.Join(dataDir, "encryption.key"), encKey, 0o600); err != nil {
			return fmt.Errorf("stage secrets: encryption.key: %w", err)
		}
	}
	if len(clusterID) > 0 {
		if err := os.WriteFile(filepath.Join(dataDir, nodeconfig.ClusterIDFile), clusterID, 0o600); err != nil {
			return fmt.Errorf("stage secrets: cluster.id: %w", err)
		}
	}
	keysDir := nodeconfig.New(dataDir).KEKDir()
	if err := os.MkdirAll(keysDir, 0o700); err != nil {
		return fmt.Errorf("stage secrets: mkdir keys: %w", err)
	}
	for _, g := range kekGens {
		name := fmt.Sprintf("%d.key", g.Gen)
		if err := os.WriteFile(filepath.Join(keysDir, name), g.Key, 0o600); err != nil {
			return fmt.Errorf("stage secrets: keys/%s: %w", name, err)
		}
	}
	return nil
}

// --- node.key.unsealed (plain PKCS#8 PEM, 0600) ----------------------------

func writeNodeKeyUnsealed(keysD, path string, priv *ecdsa.PrivateKey) error {
	if err := os.MkdirAll(keysD, 0o700); err != nil {
		return fmt.Errorf("write unsealed node key: mkdir: %w", err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return fmt.Errorf("write unsealed node key: marshal: %w", err)
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	if err := os.WriteFile(path, pemBytes, 0o600); err != nil {
		return fmt.Errorf("write unsealed node key: %w", err)
	}
	return nil
}

// shredFile best-effort overwrites + removes a small secret file.
func shredFile(path string) {
	if info, err := os.Stat(path); err == nil && info.Size() > 0 {
		if f, err := os.OpenFile(path, os.O_WRONLY, 0o600); err == nil {
			zeros := make([]byte, info.Size())
			_, _ = f.Write(zeros)
			_ = f.Sync()
			_ = f.Close()
		}
	}
	_ = os.Remove(path)
}

// --- resume sentinel (length-prefixed binary, NO JSON) ---------------------

type invitePendingRecord struct {
	inviteID string
	leaderID string
	seedAddr string
	seedSPKI [32]byte
	nodeID   string
	raftAddr string
	nodeSPKI [32]byte
}

func writeInvitePendingSentinel(dataDir, path string, st *inviteJoinState) error {
	rec := invitePendingRecord{
		inviteID: st.inviteID,
		leaderID: st.leaderID,
		seedAddr: st.seedAddr,
		seedSPKI: st.seedSPKI,
		nodeID:   st.nodeID,
		raftAddr: st.raftAddr,
		nodeSPKI: st.nodeSPKI,
	}
	var buf []byte
	buf = transport.JoinPutField(buf, []byte(rec.inviteID))
	buf = transport.JoinPutField(buf, []byte(rec.leaderID))
	buf = transport.JoinPutField(buf, []byte(rec.seedAddr))
	buf = transport.JoinPutField(buf, rec.seedSPKI[:])
	buf = transport.JoinPutField(buf, []byte(rec.nodeID))
	buf = transport.JoinPutField(buf, []byte(rec.raftAddr))
	buf = transport.JoinPutField(buf, rec.nodeSPKI[:])
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return fmt.Errorf("write invite sentinel: mkdir: %w", err)
	}
	if err := os.WriteFile(path, buf, 0o600); err != nil {
		return fmt.Errorf("write invite sentinel: %w", err)
	}
	return nil
}

func readInvitePendingSentinel(dataDir string) (invitePendingRecord, bool) {
	path := filepath.Join(dataDir, invitePendingFile)
	data, err := os.ReadFile(path)
	if err != nil {
		return invitePendingRecord{}, false
	}
	fields, err := transport.JoinReadFields(bytes.NewReader(data), 7)
	if err != nil {
		return invitePendingRecord{}, false
	}
	var rec invitePendingRecord
	rec.inviteID = string(fields[0])
	rec.leaderID = string(fields[1])
	rec.seedAddr = string(fields[2])
	copy(rec.seedSPKI[:], fields[3])
	rec.nodeID = string(fields[4])
	rec.raftAddr = string(fields[5])
	copy(rec.nodeSPKI[:], fields[6])
	return rec, true
}
