package encrypt

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildAAD_ShardDomain(t *testing.T) {
	clusterID := bytes.Repeat([]byte{0x42}, 16)
	objectID := []byte("bucket1/object_xyz")
	chunkIdx := uint32(7)

	aad := BuildAAD(DomainShard, clusterID,
		FieldBytes(objectID),
		FieldUint32(chunkIdx),
	)

	require.Truef(t, bytes.HasPrefix(aad, []byte("AAD\x01")), "missing magic prefix; got %x", aad[:4])
	gotDomain := binary.BigEndian.Uint16(aad[4:6])
	require.Equal(t, uint16(DomainShard), gotDomain)
	require.Equal(t, clusterID, aad[6:22])
	require.Equal(t, byte(2), aad[22])
}

func TestBuildAAD_AllDomainsUnique(t *testing.T) {
	clusterID := bytes.Repeat([]byte{0}, 16)
	seen := map[uint16]string{}
	for _, dom := range []struct {
		tag  AADDomain
		name string
	}{
		{DomainShard, "shard"},
		{DomainWAL, "wal"},
		{DomainSnapshotBody, "snap_body"},
		{DomainSnapshotDEK, "snap_dek"},
		{DomainJWTKey, "jwt"},
		{DomainDEKFSMWrap, "dek_wrap"},
		{DomainKEKRotate, "kek_rotate"},
		{DomainKEKCatchup, "kek_catchup"},
		{DomainFSMValue, "fsm_value"},
	} {
		if prev, ok := seen[uint16(dom.tag)]; ok {
			require.Failf(t, "duplicate domain tag", "duplicate domain_tag %#x for %s and %s", uint16(dom.tag), prev, dom.name)
		}
		seen[uint16(dom.tag)] = dom.name
		aad := BuildAAD(dom.tag, clusterID)
		require.GreaterOrEqualf(t, len(aad), 4+2+16+1, "AAD for %s too short", dom.name)
	}
}

func TestBuildAAD_FieldKinds(t *testing.T) {
	clusterID := bytes.Repeat([]byte{0}, 16)
	aad := BuildAAD(DomainShard, clusterID,
		FieldString("hello"),
		FieldBytes([]byte{0xCA, 0xFE}),
		FieldUint16(0x1234),
		FieldUint32(0xDEADBEEF),
		FieldUint64(0x0102030405060708),
	)
	require.Equal(t, byte(5), aad[22])
	last8 := aad[len(aad)-8:]
	expected := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	require.Equal(t, expected, last8)
}

func TestBuildAAD_RejectClusterIDWrongLen(t *testing.T) {
	require.Panics(t, func() {
		BuildAAD(DomainShard, []byte{0x01})
	})
}

func TestBuildAAD_AllDomainsIncludesIAMAdmin(t *testing.T) {
	clusterID := bytes.Repeat([]byte{0}, 16)
	// IAMAdmin was omitted from the original uniqueness test; lock it in
	// alongside every other domain so a future tag collision is caught.
	domains := []AADDomain{
		DomainShard, DomainWAL, DomainSnapshotBody, DomainSnapshotDEK,
		DomainJWTKey, DomainDEKFSMWrap, DomainKEKRotate, DomainKEKCatchup,
		DomainIAMAdmin, DomainCapabilityAssertV1, DomainCASChunk,
		DomainIAMCredential, DomainFSMValue, DomainClusterConfigSecret,
		DomainProtocolCredential,
	}
	seen := make(map[uint16]struct{}, len(domains))
	for _, d := range domains {
		if _, dup := seen[uint16(d)]; dup {
			require.Failf(t, "duplicate domain tag", "duplicate domain_tag %#x", uint16(d))
		}
		seen[uint16(d)] = struct{}{}
		_ = BuildAAD(d, clusterID) // exercise BuildAAD with every domain
	}
	require.Len(t, seen, 15)
}

func TestAppendAADByteIdenticalToBuildAAD(t *testing.T) {
	clusterID := bytes.Repeat([]byte{0x42}, 16)
	fields := []AADField{
		FieldString("bucket"),
		FieldUint64(42),
		FieldUint32(3),
	}
	want := BuildAAD(DomainShard, clusterID, fields...)

	prefix := []byte("PREFIX")
	dst := make([]byte, len(prefix), len(prefix)+len(want)+64)
	copy(dst, prefix)
	got := AppendAAD(dst, DomainShard, clusterID, fields...)
	require.Equal(t, prefix, got[:len(prefix)], "AppendAAD must preserve existing prefix")
	require.Equal(t, want, got[len(prefix):], "AppendAAD must append byte-identical BuildAAD output")

	gotNil := AppendAAD(nil, DomainShard, clusterID, fields...)
	require.Equal(t, want, gotNil, "AppendAAD(nil, ...) must equal BuildAAD exactly")
}

var sinkAADField AADField

func TestAADField_ConstructorsZeroAlloc(t *testing.T) {
	require.Zero(t, testing.AllocsPerRun(1000, func() { sinkAADField = FieldString("bucket/object") }), "FieldString")
	require.Zero(t, testing.AllocsPerRun(1000, func() { sinkAADField = FieldUint16(0x1234) }), "FieldUint16")
	require.Zero(t, testing.AllocsPerRun(1000, func() { sinkAADField = FieldUint32(0xDEADBEEF) }), "FieldUint32")
	require.Zero(t, testing.AllocsPerRun(1000, func() { sinkAADField = FieldUint64(0x0102030405060708) }), "FieldUint64")
}

func TestBuildAAD_ByteIdenticalAcrossKinds(t *testing.T) {
	clusterID := bytes.Repeat([]byte{0xAB}, 16)
	got := BuildAAD(DomainShard, clusterID,
		FieldString(""),                      // kind 0x01, len 0x0000, no payload
		FieldString("bk"),                    // kind 0x01, len 0x0002, "bk"
		FieldBytes([]byte{0xCA, 0xFE, 0xBA}), // kind 0x02, len 0x0003, CA FE BA
		FieldUint16(0),                       // kind 0x03, len 0x0002, 00 00
		FieldUint32(0x01020304),              // kind 0x04, len 0x0004, 01 02 03 04
		FieldUint64(0xA1A2A3A4A5A6A7A8),      // kind 0x05, len 0x0008, A1..A8
	)
	want := []byte{ /* "AAD\x01" */ 0x41, 0x41, 0x44, 0x01,
		/* domain DomainShard=0x0001 */ 0x00, 0x01,
		/* clusterID 16x 0xAB */ 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB,
		/* fieldCount */ 0x06,
		/* FieldString "" */ 0x01, 0x00, 0x00,
		/* FieldString "bk" */ 0x01, 0x00, 0x02, 0x62, 0x6B,
		/* FieldBytes CAFEBA */ 0x02, 0x00, 0x03, 0xCA, 0xFE, 0xBA,
		/* FieldUint16 0 */ 0x03, 0x00, 0x02, 0x00, 0x00,
		/* FieldUint32 */ 0x04, 0x00, 0x04, 0x01, 0x02, 0x03, 0x04,
		/* FieldUint64 */ 0x05, 0x00, 0x08, 0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7, 0xA8,
	}
	require.Equal(t, want, got)
}

func TestBuildAAD_FieldBytesDefensiveCopy(t *testing.T) {
	// The AAD format is consensus-critical: if FieldBytes ever loses its
	// defensive copy, callers can silently produce divergent AADs by
	// mutating their input slice between FieldBytes and BuildAAD. This
	// test locks the invariant.
	clusterID := bytes.Repeat([]byte{0}, 16)
	input := []byte{0x01, 0x02, 0x03}
	field := FieldBytes(input)

	// Mutate the input AFTER constructing the field but BEFORE BuildAAD.
	input[0] = 0x99

	aad := BuildAAD(DomainShard, clusterID, field)

	// Last 3 bytes of aad must be the ORIGINAL {0x01, 0x02, 0x03}, not the
	// post-mutation value.
	last3 := aad[len(aad)-3:]
	want := []byte{0x01, 0x02, 0x03}
	require.Equal(t, want, last3)
}
