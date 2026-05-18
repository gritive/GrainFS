package cluster

import "testing"

func BenchmarkMetaJoinRequest_RoundTrip(b *testing.B) {
	req := JoinRequest{NodeID: "node-1", Address: "10.0.0.1:9100"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		data, err := encodeJoinRequest(req)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := decodeJoinRequest(data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMetaJoinReply_RoundTrip(b *testing.B) {
	cases := []struct {
		name  string
		reply JoinReply
	}{
		{"ok", JoinReply{Accepted: true, Status: JoinStatusOK, LeaderID: "leader-1", LeaderAddr: "10.0.0.2:9100"}},
		{"not-leader", JoinReply{Status: JoinStatusNotLeader, Message: "follow leader", LeaderID: "leader-1", LeaderAddr: "10.0.0.2:9100"}},
	}
	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				data, err := encodeJoinReply(tc.reply)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := decodeJoinReply(data); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
