package serveruntime

// normalizeIndexGroupCount clamps the configured object-index group count to a
// sane floor. Zero/negative (unset) -> 1, the behavior-neutral single-shard
// meta-FSM path. >1 activates the sharded index-group boot path.
func normalizeIndexGroupCount(n int) int {
	if n < 1 {
		return 1
	}
	return n
}
