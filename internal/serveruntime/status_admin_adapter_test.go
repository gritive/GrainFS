package serveruntime

import (
	"testing"
)

func TestDerivePhase(t *testing.T) {
	tests := []struct {
		name           string
		saCount        int
		clusterSize    int
		tlsCertPresent bool
		want           int
	}{
		{
			name:           "single-node no SA → phase 0",
			saCount:        0,
			clusterSize:    1,
			tlsCertPresent: false,
			want:           0,
		},
		{
			name:           "cluster no SA → phase 1",
			saCount:        0,
			clusterSize:    3,
			tlsCertPresent: false,
			want:           1,
		},
		{
			name:           "SA bootstrapped no TLS → phase 2",
			saCount:        1,
			clusterSize:    1,
			tlsCertPresent: false,
			want:           2,
		},
		{
			name:           "SA bootstrapped with TLS → phase 3",
			saCount:        1,
			clusterSize:    1,
			tlsCertPresent: true,
			want:           3,
		},
		{
			name:           "cluster with SA but no TLS → phase 2",
			saCount:        2,
			clusterSize:    5,
			tlsCertPresent: false,
			want:           2,
		},
		{
			name:           "cluster with SA and TLS → phase 3",
			saCount:        3,
			clusterSize:    5,
			tlsCertPresent: true,
			want:           3,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := derivePhase(tc.saCount, tc.clusterSize, tc.tlsCertPresent)
			if got != tc.want {
				t.Errorf("derivePhase(%d, %d, %v) = %d, want %d",
					tc.saCount, tc.clusterSize, tc.tlsCertPresent, got, tc.want)
			}
		})
	}
}
