package cluster

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/iam/bucketpolicy"
	"github.com/gritive/GrainFS/internal/iam/group"
	"github.com/gritive/GrainFS/internal/iam/mountsastore"
	"github.com/gritive/GrainFS/internal/iam/policyattach"
	"github.com/gritive/GrainFS/internal/iam/policystore"
)

// iamSnapshotTrailerMagic is appended after the IAM section so post-fix
// readers can distinguish "new snapshot with IAM trailer" from a legacy
// snapshot that ends at the FlatBuffer root. ASCII "IAMG" (0x47414D49 little-endian).
const iamSnapshotTrailerMagic uint32 = 0x47414D49

// iamSnapshotTrailerLen is the on-disk size of the trailer footer:
// [u32 iam_len][u32 magic]. Always 8 bytes when present.
const iamSnapshotTrailerLen = 8

// cfgSnapshotTrailerMagic identifies the GCFG trailer appended after the IAM
// trailer (or after the FB root when IAM is absent). Hex pairs spell "GCFG"
// (0x47=G, 0x43=C, 0x46=F, 0x47=G, little-endian uint32 = 0x47464347).
//
// Wire layout (appended after existing trailers):
//
//	[FB root bytes]
//	[IAM trailer bytes]       (optional, magic 0x47414D49)
//	[configPayloadBytes]
//	[uint32 payloadLen  LE]
//	[uint32 magic GCFG  LE]
//
// Restore reads from the end: check last 4 bytes for GCFG magic, if present
// read payloadLen, strip GCFG payload+footer, then continue IAM detection on
// the remaining bytes.
const cfgSnapshotTrailerMagic uint32 = 0x47464347

// cfgSnapshotTrailerLen is the on-disk size of the GCFG footer:
// [u32 payload_len][u32 magic]. Always 8 bytes when present.
const cfgSnapshotTrailerLen = 8

// dekSnapshotTrailerMagic identifies the DKVS trailer appended after the GCFG
// trailer (or after IAM when GCFG absent, or after FB root when both absent).
// Hex pairs spell "DKVS" (0x44=D, 0x4B=K, 0x56=V, 0x53=S, little-endian uint32).
//
// Wire layout (appended after existing trailers):
//
//	[FB root bytes]
//	[IAM trailer bytes]       (optional, magic 0x47414D49)
//	[GCFG trailer bytes]      (optional, magic 0x47464347)
//	[dekPayloadBytes]
//	[uint32 payloadLen  LE]
//	[uint32 magic DKVS  LE]
//
// Restore reads from the end: check last 4 bytes for DKVS magic, if present
// read payloadLen, strip DKVS payload+footer, then continue GCFG/IAM detection.
const dekSnapshotTrailerMagic uint32 = 0x53564B44

// dekSnapshotTrailerLen is the on-disk size of the DKVS footer:
// [u32 payload_len][u32 magic]. Always 8 bytes when present.
const dekSnapshotTrailerLen = 8

// ipstSnapshotTrailerMagic identifies the IPST trailer appended after the DKVS
// trailer (or after whichever trailers precede it). Hex pairs spell "IPST"
// (0x49=I, 0x50=P, 0x53=S, 0x54=T -> little-endian uint32 = 0x54535049).
//
// Carries: PolicyStore + GroupStore + PolicyAttachStore + BucketPolicyStore in
// a single FlatBuffers payload. One trailer for all 4 stores keeps the chain
// depth manageable and reflects that the stores always serialize together.
//
// Wire layout (appended before JKEY; JKEY is now the outermost trailer):
//
//	[FB root bytes]
//	[IAM trailer bytes]       (optional, magic 0x47414D49)
//	[GCFG trailer bytes]      (optional, magic 0x47464347)
//	[DKVS trailer bytes]      (optional, magic 0x53564B44)
//	[ipstPayloadBytes]
//	[uint32 payloadLen  LE]
//	[uint32 magic IPST  LE]
//
// Restore reads from the end: peel JKEY first, then check for IPST magic,
// if present read payloadLen, strip IPST payload+footer, then continue DKVS/GCFG/IAM peel.
const ipstSnapshotTrailerMagic uint32 = 0x54535049

// ipstSnapshotTrailerLen is the on-disk size of the IPST footer:
// [u32 payload_len][u32 magic]. Always 8 bytes when present.
const ipstSnapshotTrailerLen = 8

// jkeySnapshotTrailerMagic identifies the JKEY trailer appended after the IPST
// trailer (outermost trailer). Hex pairs spell "JKEY"
// (0x4A=J, 0x4B=K, 0x45=E, 0x59=Y, little-endian uint32 = 0x59454B4A).
//
// Carries: JWTKeyStore - current + previous wrapped JWT signing keys.
// Only appended when at least one key exists (nil/nil is skipped for back-compat).
//
// Wire layout (JKEY is the newest/outermost trailer):
//
//	[FB root bytes]
//	[IAM trailer bytes]       (optional, magic 0x47414D49)
//	[GCFG trailer bytes]      (optional, magic 0x47464347)
//	[DKVS trailer bytes]      (optional, magic 0x53564B44)
//	[IPST trailer bytes]      (optional, magic 0x54535049)
//	[jkeyPayloadBytes]
//	[uint32 payloadLen  LE]
//	[uint32 magic JKEY  LE]
//
// Restore reads from the end: check last 4 bytes for JKEY magic, if present
// read payloadLen, strip JKEY payload+footer, then continue IPST/DKVS/GCFG/IAM peel.
const jkeySnapshotTrailerMagic uint32 = 0x59454B4A

// jkeySnapshotTrailerLen is the on-disk size of the JKEY footer:
// [u32 payload_len][u32 magic]. Always 8 bytes when present.
const jkeySnapshotTrailerLen = 8

type metaSnapshotTrailers struct {
	fbData   []byte
	iamData  []byte
	cfgData  []byte
	dekData  []byte
	ipstData []byte
	jkeyData []byte
}

func appendSnapshotTrailer(out, payload []byte, footerLen int, magic uint32) []byte {
	out = append(out, payload...)
	footer := make([]byte, footerLen)
	binary.LittleEndian.PutUint32(footer[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(footer[4:8], magic)
	return append(out, footer...)
}

func peelSnapshotTrailer(in []byte, footerLen int, magic uint32, name string) (remaining, payload []byte, present bool, err error) {
	if len(in) < footerLen {
		return in, nil, false, nil
	}
	footer := in[len(in)-footerLen:]
	if binary.LittleEndian.Uint32(footer[4:8]) != magic {
		return in, nil, false, nil
	}
	payloadLen := binary.LittleEndian.Uint32(footer[0:4])
	if int(payloadLen)+footerLen > len(in) {
		return nil, nil, false, fmt.Errorf("meta_fsm: Restore: %s trailer length %d exceeds snapshot size %d", name, payloadLen, len(in))
	}
	payloadEnd := len(in) - footerLen
	payloadStart := payloadEnd - int(payloadLen)
	return in[:payloadStart], in[payloadStart:payloadEnd], true, nil
}

func (f *MetaFSM) appendIAMSnapshotTrailer(out []byte) ([]byte, error) {
	var iamBytes []byte
	if f.iamStore != nil {
		var buf bytes.Buffer
		if err := iam.WriteSnapshot(&buf, f.iamStore); err != nil {
			return nil, fmt.Errorf("meta_fsm: Snapshot: encode IAM: %w", err)
		}
		iamBytes = buf.Bytes()
	}
	return appendSnapshotTrailer(out, iamBytes, iamSnapshotTrailerLen, iamSnapshotTrailerMagic), nil
}

func (f *MetaFSM) appendConfigSnapshotTrailer(out []byte) ([]byte, error) {
	if f.cfgStore == nil {
		return out, nil
	}
	cfgValues := f.cfgStore.Snapshot()
	if len(cfgValues) == 0 {
		return out, nil
	}
	cfgPayload, err := encodeMetaConfigSnapshot(cfgValues)
	if err != nil {
		return nil, fmt.Errorf("meta_fsm: Snapshot: encode config: %w", err)
	}
	return appendSnapshotTrailer(out, cfgPayload, cfgSnapshotTrailerLen, cfgSnapshotTrailerMagic), nil
}

// appendDEKSnapshotTrailer serialises the DKVS trailer using caller-supplied
// pre-captured values from the locked snapshot window. It does NOT call back
// into DEKKeeper — callers must pass values captured while holding f.mu (Task 4b).
func (f *MetaFSM) appendDEKSnapshotTrailer(out []byte, dekVersions map[uint32][]byte, dekActive uint32, refCounts map[uint32]uint64, activeKEKVersion uint32) ([]byte, error) {
	if len(dekVersions) == 0 {
		return out, nil
	}
	dekPayload, err := encodeMetaDEKVersionSnapshot(dekVersions, dekActive, refCounts, activeKEKVersion)
	if err != nil {
		return nil, fmt.Errorf("meta_fsm: Snapshot: encode DEK versions: %w", err)
	}
	return appendSnapshotTrailer(out, dekPayload, dekSnapshotTrailerLen, dekSnapshotTrailerMagic), nil
}

func (f *MetaFSM) appendPolicyStoresSnapshotTrailer(out []byte) ([]byte, error) {
	if f.policyStore == nil && f.groupStore == nil && f.policyAttachStore == nil && f.bucketPolicyStore == nil && f.mountSAStore == nil {
		return out, nil
	}
	var polSnap []policystore.PolicyEntry
	if f.policyStore != nil {
		polSnap = f.policyStore.Snapshot()
	}
	var grpSnap []group.GroupEntry
	if f.groupStore != nil {
		grpSnap = f.groupStore.Snapshot()
	}
	var attachSnap policyattach.AttachSnapshot
	if f.policyAttachStore != nil {
		attachSnap = f.policyAttachStore.Snapshot()
	}
	var bpSnap []bucketpolicy.BucketPolicyEntry
	if f.bucketPolicyStore != nil {
		bpSnap = f.bucketPolicyStore.Snapshot()
	}
	var mountSASnap []mountsastore.MountSA
	if f.mountSAStore != nil {
		mountSASnap = f.mountSAStore.ListAll()
	}
	ipstPayload, err := encodeMetaIAMPolicyStoresSnapshot(polSnap, grpSnap, attachSnap, bpSnap, mountSASnap)
	if err != nil {
		return nil, fmt.Errorf("meta_fsm: Snapshot: encode IAM policy stores: %w", err)
	}
	return appendSnapshotTrailer(out, ipstPayload, ipstSnapshotTrailerLen, ipstSnapshotTrailerMagic), nil
}

func (f *MetaFSM) appendJWTKeySnapshotTrailer(out []byte) []byte {
	jkeyCurrent, jkeyPrevious := f.jwtKeyStore.Snapshot()
	if jkeyCurrent == nil && jkeyPrevious == nil {
		return out
	}
	jkeyPayload := encodeJWTKeyStore(jkeyCurrent, jkeyPrevious)
	return appendSnapshotTrailer(out, jkeyPayload, jkeySnapshotTrailerLen, jkeySnapshotTrailerMagic)
}

// appendSnapshotTrailers serialises all snapshot trailers. dekVersions,
// dekActive, refCounts, and activeKEKVersion must be pre-captured inside the
// f.mu+keeper.mu locked window in Snapshot() (Task 4b atomicity guarantee).
func (f *MetaFSM) appendSnapshotTrailers(base []byte, dekVersions map[uint32][]byte, dekActive uint32, refCounts map[uint32]uint64, activeKEKVersion uint32) ([]byte, error) {
	out := append([]byte(nil), base...)
	var err error
	out, err = f.appendIAMSnapshotTrailer(out)
	if err != nil {
		return nil, err
	}
	out, err = f.appendConfigSnapshotTrailer(out)
	if err != nil {
		return nil, err
	}
	out, err = f.appendDEKSnapshotTrailer(out, dekVersions, dekActive, refCounts, activeKEKVersion)
	if err != nil {
		return nil, err
	}
	out, err = f.appendPolicyStoresSnapshotTrailer(out)
	if err != nil {
		return nil, err
	}
	out = f.appendJWTKeySnapshotTrailer(out)
	return out, nil
}

func peelMetaSnapshotTrailers(data []byte) (metaSnapshotTrailers, error) {
	remaining := data
	out := metaSnapshotTrailers{}
	var err error

	// Peel in reverse append order. Each successful peel exposes the next-older
	// trailer as the end of the remaining byte slice.
	remaining, out.jkeyData, _, err = peelSnapshotTrailer(remaining, jkeySnapshotTrailerLen, jkeySnapshotTrailerMagic, "JKEY")
	if err != nil {
		return out, err
	}
	remaining, out.ipstData, _, err = peelSnapshotTrailer(remaining, ipstSnapshotTrailerLen, ipstSnapshotTrailerMagic, "IPST")
	if err != nil {
		return out, err
	}
	remaining, out.dekData, _, err = peelSnapshotTrailer(remaining, dekSnapshotTrailerLen, dekSnapshotTrailerMagic, "DKVS")
	if err != nil {
		return out, err
	}
	remaining, out.cfgData, _, err = peelSnapshotTrailer(remaining, cfgSnapshotTrailerLen, cfgSnapshotTrailerMagic, "GCFG")
	if err != nil {
		return out, err
	}
	remaining, out.iamData, _, err = peelSnapshotTrailer(remaining, iamSnapshotTrailerLen, iamSnapshotTrailerMagic, "IAM")
	if err != nil {
		return out, err
	}
	out.fbData = remaining
	return out, nil
}
