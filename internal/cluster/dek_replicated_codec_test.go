package cluster

import (
	"math"
	"testing"
)

func TestDEKReplicatedRotateCmd_RoundTrip(t *testing.T) {
	in := DEKReplicatedRotateCmd{
		Gen:               3,
		WrappedDEK:        []byte("wrapped-dek-bytes-under-active-kek"),
		ExpectedActiveGen: 2,
		ActiveKEKVer:      2,
	}
	enc, err := EncodeDEKReplicatedRotateCmd(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	out, err := DecodeDEKReplicatedRotateCmd(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Gen != in.Gen || out.ExpectedActiveGen != in.ExpectedActiveGen ||
		out.ActiveKEKVer != in.ActiveKEKVer || string(out.WrappedDEK) != string(in.WrappedDEK) {
		t.Fatalf("round-trip mismatch: %+v vs %+v", out, in)
	}
}

func TestDEKReplicatedRotateCmd_BootstrapSentinel(t *testing.T) {
	// gen-0 bootstrap carries ExpectedActiveGen = math.MaxUint32 ("no existing DEK").
	in := DEKReplicatedRotateCmd{Gen: 0, WrappedDEK: []byte("w"), ExpectedActiveGen: math.MaxUint32, ActiveKEKVer: 0}
	enc, _ := EncodeDEKReplicatedRotateCmd(in)
	out, _ := DecodeDEKReplicatedRotateCmd(enc)
	if out.ExpectedActiveGen != math.MaxUint32 {
		t.Fatalf("sentinel not preserved: %d", out.ExpectedActiveGen)
	}
}

func TestDEKReplicatedRotateCmd_RejectsEmptyWrapped(t *testing.T) {
	if _, err := EncodeDEKReplicatedRotateCmd(DEKReplicatedRotateCmd{Gen: 1, ExpectedActiveGen: 0}); err == nil {
		t.Fatal("expected error on empty WrappedDEK")
	}
}
