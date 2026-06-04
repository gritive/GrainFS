package cluster

import "fmt"

// stripeFragSize: per-shard fragment for a stripe holding stripeDataLen bytes under k data shards.
func stripeFragSize(stripeDataLen, k int) int {
	return (stripeDataLen + k - 1) / k
}

func numStripes(objectSize int64, stripeBytes int) int {
	if stripeBytes <= 0 {
		return 0
	}
	return int((objectSize + int64(stripeBytes) - 1) / int64(stripeBytes))
}

// stripeDeinterleave reconstructs the object from stripe-INTERLEAVED shard bodies
// (8-byte header already stripped). bodies has length K+M; nil entry = missing shard
// (RS reconstruct per stripe). Result is exactly objectSize bytes.
func stripeDeinterleave(cfg ECConfig, bodies [][]byte, stripeBytes int, objectSize int64) ([]byte, error) {
	k, n := cfg.DataShards, cfg.NumShards()
	if len(bodies) != n {
		return nil, fmt.Errorf("stripe de-interleave: got %d shards, want %d", len(bodies), n)
	}
	if stripeBytes <= 0 {
		return nil, fmt.Errorf("stripe de-interleave: stripeBytes must be > 0")
	}
	enc, err := getEncoder(cfg)
	if err != nil {
		return nil, fmt.Errorf("stripe de-interleave encoder: %w", err)
	}
	out := make([]byte, 0, objectSize)
	stripes := numStripes(objectSize, stripeBytes)
	offset := 0
	remaining := int(objectSize)
	for s := 0; s < stripes; s++ {
		dS := stripeBytes
		if remaining < dS {
			dS = remaining
		}
		fragSize := stripeFragSize(dS, k)
		frags := make([][]byte, n)
		missing := false
		for i := 0; i < n; i++ {
			if bodies[i] == nil {
				missing = true
				continue
			}
			if offset+fragSize > len(bodies[i]) {
				return nil, fmt.Errorf("stripe de-interleave: shard %d short at stripe %d (have %d need %d)", i, s, len(bodies[i]), offset+fragSize)
			}
			frags[i] = bodies[i][offset : offset+fragSize]
		}
		if missing {
			rebuilt := make([][]byte, n)
			for i := 0; i < n; i++ {
				if frags[i] != nil {
					rebuilt[i] = append([]byte(nil), frags[i]...)
				}
			}
			if err := enc.Reconstruct(rebuilt); err != nil {
				return nil, fmt.Errorf("stripe de-interleave reconstruct stripe %d: %w", s, err)
			}
			frags = rebuilt
		}
		for kk := 0; kk < k; kk++ {
			out = append(out, frags[kk]...)
		}
		out = out[:len(out)-(k*fragSize-dS)] // trim intra-stripe padding (zeros in last data shard tail)
		offset += fragSize
		remaining -= dS
	}
	return out, nil
}
