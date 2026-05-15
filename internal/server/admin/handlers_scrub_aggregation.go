package admin

import "context"

func aggregateScrubPeers(ctx context.Context, d *Deps, sessionID string) ([]ScrubJobInfo, []string, error) {
	if d.ScrubAggregator == nil {
		return nil, nil, nil
	}
	peerInfos, peerFailures, err := d.ScrubAggregator.Peers(ctx, sessionID)
	if err != nil {
		return nil, nil, NewInternal("aggregate peers: " + err.Error())
	}
	return peerInfos, peerFailures, nil
}

func mergeScrubPeerInfo(out ScrubJobInfo, peerInfos []ScrubJobInfo, peerFailures []string) ScrubJobInfo {
	for _, p := range peerInfos {
		out.Checked += p.Checked
		out.Healthy += p.Healthy
		out.Detected += p.Detected
		out.Repaired += p.Repaired
		out.Unrepairable += p.Unrepairable
		out.Skipped += p.Skipped
		if p.Status == "running" {
			out.Status = "running"
		}
		if p.OwnedHere && (out.Bucket == "" || out.Bucket == "—") {
			out.Bucket = p.Bucket
			out.KeyPrefix = p.KeyPrefix
			out.Scope = p.Scope
			out.DryRun = p.DryRun
		}
	}
	if out.Status == "" {
		for _, p := range peerInfos {
			if p.Status != "" {
				out.Status = p.Status
				break
			}
		}
	}
	if len(peerFailures) > 0 {
		out.Partial = true
		out.PeerFailures = peerFailures
	}
	return out
}
