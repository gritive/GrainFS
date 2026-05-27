package encrypt

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestBuildAAD_ShardDomain(t *testing.T) {
	clusterID := bytes.Repeat([]byte{0x42}, 16)
	objectID := []byte("bucket1/object_xyz")
	chunkIdx := uint32(7)

	aad := BuildAAD(DomainShard, clusterID,
		FieldBytes(objectID),
		FieldUint32(chunkIdx),
	)

	if !bytes.HasPrefix(aad, []byte("AAD\x01")) {
		t.Fatalf("missing magic prefix; got %x", aad[:4])
	}
	gotDomain := binary.BigEndian.Uint16(aad[4:6])
	if gotDomain != uint16(DomainShard) {
		t.Errorf("domain = %#x, want %#x", gotDomain, uint16(DomainShard))
	}
	if !bytes.Equal(aad[6:22], clusterID) {
		t.Errorf("cluster_id mismatch")
	}
	if aad[22] != 2 {
		t.Errorf("num_fields = %d, want 2", aad[22])
	}
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
		{DomainNBD, "nbd"},
	} {
		if prev, ok := seen[uint16(dom.tag)]; ok {
			t.Errorf("duplicate domain_tag %#x for %s and %s", uint16(dom.tag), prev, dom.name)
		}
		seen[uint16(dom.tag)] = dom.name
		aad := BuildAAD(dom.tag, clusterID)
		if len(aad) < 4+2+16+1 {
			t.Errorf("AAD for %s too short: %d bytes", dom.name, len(aad))
		}
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
	if aad[22] != 5 {
		t.Fatalf("num_fields = %d, want 5", aad[22])
	}
	last8 := aad[len(aad)-8:]
	expected := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	if !bytes.Equal(last8, expected) {
		t.Errorf("uint64 BE encoding wrong: %x", last8)
	}
}

func TestBuildAAD_RejectClusterIDWrongLen(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic for cluster_id len != 16")
		}
	}()
	BuildAAD(DomainShard, []byte{0x01})
}

func TestBuildAAD_AllDomainsIncludesIAMAdmin(t *testing.T) {
	clusterID := bytes.Repeat([]byte{0}, 16)
	// IAMAdmin was omitted from the original uniqueness test; lock it in
	// alongside every other domain so a future tag collision is caught.
	domains := []AADDomain{
		DomainShard, DomainWAL, DomainSnapshotBody, DomainSnapshotDEK,
		DomainJWTKey, DomainDEKFSMWrap, DomainKEKRotate, DomainKEKCatchup,
		DomainNBD, DomainIAMAdmin, DomainCapabilityAssertV1,
	}
	seen := make(map[uint16]struct{}, len(domains))
	for _, d := range domains {
		if _, dup := seen[uint16(d)]; dup {
			t.Errorf("duplicate domain_tag %#x", uint16(d))
		}
		seen[uint16(d)] = struct{}{}
		_ = BuildAAD(d, clusterID) // exercise BuildAAD with every domain
	}
	if len(seen) != 11 {
		t.Errorf("expected 11 unique domains, got %d", len(seen))
	}
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
	if !bytes.Equal(last3, want) {
		t.Errorf("FieldBytes did not defensively copy: got %x, want %x", last3, want)
	}
}
