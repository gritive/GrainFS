package cluster

import (
	"fmt"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// encodeMetaRotateKeyBeginCmd serializes a RotateKeyBegin payload (raft cmd
// data — wrap with encodeMetaCmd to get the MetaCmd envelope).
func encodeMetaRotateKeyBeginCmd(c RotateKeyBegin) ([]byte, error) {
	b := clusterBuilderPool.Get()
	rotIDOff := b.CreateByteVector(c.RotationID[:])
	spkiOff := b.CreateByteVector(c.ExpectedNewSPKI[:])
	auditOff := b.CreateByteVector(c.AuditNewSPKIHash[:])
	startedByOff := b.CreateString(c.StartedBy)
	clusterpb.MetaRotateKeyBeginCmdStart(b)
	clusterpb.MetaRotateKeyBeginCmdAddRotationId(b, rotIDOff)
	clusterpb.MetaRotateKeyBeginCmdAddExpectedNewSpki(b, spkiOff)
	clusterpb.MetaRotateKeyBeginCmdAddAuditNewSpkiHash(b, auditOff)
	clusterpb.MetaRotateKeyBeginCmdAddStartedBy(b, startedByOff)
	clusterpb.MetaRotateKeyBeginCmdAddCapabilities(b, c.Capabilities)
	return fbFinish(b, clusterpb.MetaRotateKeyBeginCmdEnd(b)), nil
}

func decodeMetaRotateKeyBeginCmd(data []byte) (RotateKeyBegin, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaRotateKeyBeginCmd {
		return clusterpb.GetRootAsMetaRotateKeyBeginCmd(d, 0)
	})
	if err != nil {
		return RotateKeyBegin{}, err
	}
	out := RotateKeyBegin{
		StartedBy:    string(t.StartedBy()),
		Capabilities: t.Capabilities(),
	}
	if rid := t.RotationIdBytes(); len(rid) == 16 {
		copy(out.RotationID[:], rid)
	} else {
		return RotateKeyBegin{}, fmt.Errorf("rotation_codec: RotationID must be 16 bytes, got %d", len(rid))
	}
	if spki := t.ExpectedNewSpkiBytes(); len(spki) == 32 {
		copy(out.ExpectedNewSPKI[:], spki)
	} else {
		return RotateKeyBegin{}, fmt.Errorf("rotation_codec: ExpectedNewSPKI must be 32 bytes, got %d", len(spki))
	}
	if audit := t.AuditNewSpkiHashBytes(); len(audit) == 32 {
		copy(out.AuditNewSPKIHash[:], audit)
	} else if len(audit) != 0 {
		return RotateKeyBegin{}, fmt.Errorf("rotation_codec: AuditNewSPKIHash must be 32 bytes or empty, got %d", len(audit))
	}
	return out, nil
}

func encodeMetaRotateKeySwitchCmd(c RotateKeySwitch) ([]byte, error) {
	b := clusterBuilderPool.Get()
	rotIDOff := b.CreateByteVector(c.RotationID[:])
	clusterpb.MetaRotateKeySwitchCmdStart(b)
	clusterpb.MetaRotateKeySwitchCmdAddRotationId(b, rotIDOff)
	return fbFinish(b, clusterpb.MetaRotateKeySwitchCmdEnd(b)), nil
}

func decodeMetaRotateKeySwitchCmd(data []byte) (RotateKeySwitch, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaRotateKeySwitchCmd {
		return clusterpb.GetRootAsMetaRotateKeySwitchCmd(d, 0)
	})
	if err != nil {
		return RotateKeySwitch{}, err
	}
	var out RotateKeySwitch
	if rid := t.RotationIdBytes(); len(rid) == 16 {
		copy(out.RotationID[:], rid)
	} else {
		return RotateKeySwitch{}, fmt.Errorf("rotation_codec: Switch RotationID must be 16 bytes, got %d", len(rid))
	}
	return out, nil
}

func encodeMetaRotateKeyDropCmd(c RotateKeyDrop) ([]byte, error) {
	b := clusterBuilderPool.Get()
	rotIDOff := b.CreateByteVector(c.RotationID[:])
	clusterpb.MetaRotateKeyDropCmdStart(b)
	clusterpb.MetaRotateKeyDropCmdAddRotationId(b, rotIDOff)
	clusterpb.MetaRotateKeyDropCmdAddGraceUntil(b, c.GraceUntil)
	return fbFinish(b, clusterpb.MetaRotateKeyDropCmdEnd(b)), nil
}

func decodeMetaRotateKeyDropCmd(data []byte) (RotateKeyDrop, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaRotateKeyDropCmd {
		return clusterpb.GetRootAsMetaRotateKeyDropCmd(d, 0)
	})
	if err != nil {
		return RotateKeyDrop{}, err
	}
	out := RotateKeyDrop{GraceUntil: t.GraceUntil()}
	if rid := t.RotationIdBytes(); len(rid) == 16 {
		copy(out.RotationID[:], rid)
	} else {
		return RotateKeyDrop{}, fmt.Errorf("rotation_codec: Drop RotationID must be 16 bytes, got %d", len(rid))
	}
	return out, nil
}

func encodeMetaRotateKeyAbortCmd(c RotateKeyAbort) ([]byte, error) {
	b := clusterBuilderPool.Get()
	rotIDOff := b.CreateByteVector(c.RotationID[:])
	reasonOff := b.CreateString(c.Reason)
	clusterpb.MetaRotateKeyAbortCmdStart(b)
	clusterpb.MetaRotateKeyAbortCmdAddRotationId(b, rotIDOff)
	clusterpb.MetaRotateKeyAbortCmdAddReason(b, reasonOff)
	return fbFinish(b, clusterpb.MetaRotateKeyAbortCmdEnd(b)), nil
}

func decodeMetaRotateKeyAbortCmd(data []byte) (RotateKeyAbort, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaRotateKeyAbortCmd {
		return clusterpb.GetRootAsMetaRotateKeyAbortCmd(d, 0)
	})
	if err != nil {
		return RotateKeyAbort{}, err
	}
	out := RotateKeyAbort{Reason: string(t.Reason())}
	if rid := t.RotationIdBytes(); len(rid) == 16 {
		copy(out.RotationID[:], rid)
	} else {
		return RotateKeyAbort{}, fmt.Errorf("rotation_codec: Abort RotationID must be 16 bytes, got %d", len(rid))
	}
	return out, nil
}

// fbFinish + fbSafe + clusterBuilderPool are defined in meta_fsm.go.
