package adminapi

import (
	"encoding/json"
	"testing"
)

func TestStorageBucketSummaryJSONShape(t *testing.T) {
	payload, err := json.Marshal(StorageBucketSummary{
		Name:        "logs",
		HasUpstream: true,
		NFSExport: &StorageBucketNFSExport{
			Registered: true,
			ReadOnly:   true,
			Generation: 7,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	got := string(payload)
	want := `{"name":"logs","has_upstream":true,"nfs_export":{"registered":true,"read_only":true,"generation":7}}`
	if got != want {
		t.Fatalf("json = %s, want %s", got, want)
	}
}

func TestStorageProtocolStatusJSONShape(t *testing.T) {
	payload, err := json.Marshal(StorageProtocolStatusResp{
		NFS4: ProtocolEndpointStatus{Enabled: true, Port: 2049},
		NBD:  ProtocolEndpointStatus{Enabled: false},
		P9:   ProtocolEndpointStatus{Enabled: true, Bind: "127.0.0.1", Port: 564},
	})
	if err != nil {
		t.Fatal(err)
	}
	var decoded StorageProtocolStatusResp
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	if !decoded.NFS4.Enabled || decoded.NFS4.Port != 2049 {
		t.Fatalf("nfs4 decoded = %+v", decoded.NFS4)
	}
	if decoded.NBD.Enabled {
		t.Fatalf("nbd decoded = %+v", decoded.NBD)
	}
	if decoded.P9.Bind != "127.0.0.1" || decoded.P9.Port != 564 {
		t.Fatalf("p9 decoded = %+v", decoded.P9)
	}
}
