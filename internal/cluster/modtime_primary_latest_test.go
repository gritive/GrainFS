package cluster

// Unit RED→GREEN for the ModTime-primary latest-version rule. The canonical
// comparator is quorumMetaBlobWins (higher ModTime; tie → higher VersionID;
// tie → higher MetaSeq). These tests assert the rule holds at the latest-deciding
// helpers regardless of UUIDv7 create-time order, so a multipart created BEFORE a
// PUT but COMPLETED after (lower VID, higher ModTime) is correctly latest.

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLatestWins_ModTimePrimary(t *testing.T) {
	// vidLow lexicographically precedes vidHigh (UUIDv7 create-time order).
	const vidLow, vidHigh = "0000-aaaa", "9999-zzzz"

	// Higher ModTime wins even with the lower (older create-time) VID.
	require.True(t, latestWins(200, vidLow, 100, vidHigh),
		"higher ModTime must win regardless of VersionID order")
	require.False(t, latestWins(100, vidHigh, 200, vidLow),
		"lower ModTime must lose even with the larger VersionID")

	// Equal ModTime (second granularity) → lexicographically greater VID wins.
	require.True(t, latestWins(100, vidHigh, 100, vidLow),
		"on an equal ModTime the greater VersionID wins")
	require.False(t, latestWins(100, vidLow, 100, vidHigh))
}

func TestDeriveLatestVersion_ModTimePrimary(t *testing.T) {
	// mpu was CREATED first (smaller VID) but COMPLETED last (larger ModTime);
	// put has the larger VID but the earlier ModTime. ModTime-primary → mpu latest.
	mpu := PutObjectMetaCmd{Key: "k", VersionID: "0000-mpu", ModTime: 200, ETag: "e-mpu"}
	put := PutObjectMetaCmd{Key: "k", VersionID: "9999-put", ModTime: 100, ETag: "e-put"}

	latest, live := deriveLatestVersion([]PutObjectMetaCmd{put, mpu})
	require.True(t, live)
	require.Equal(t, "0000-mpu", latest.VersionID,
		"the later-COMPLETED version (higher ModTime) must be latest, not the larger VID")

	// Order independence: same result regardless of scan order.
	latest2, _ := deriveLatestVersion([]PutObjectMetaCmd{mpu, put})
	require.Equal(t, latest.VersionID, latest2.VersionID)
}
