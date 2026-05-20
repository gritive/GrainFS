// Test-only helpers exported for cross-package tests (serveruntime drives
// the production wiring path and needs to encode/decode JoinRequest /
// JoinReply / ChallengeRequest / ChallengeReply without re-implementing
// the flatbuffer layouts). Keep ForTest suffix so they cannot be mistaken
// for production API.
package cluster

func EncodeJoinRequestForTest(req JoinRequest) ([]byte, error) {
	return encodeJoinRequest(req)
}

func DecodeJoinReplyForTest(data []byte) (*JoinReply, error) {
	return decodeJoinReply(data)
}

func EncodeChallengeRequestForTest(req ChallengeRequest) ([]byte, error) {
	return encodeChallengeRequest(req)
}

func DecodeChallengeReplyForTest(data []byte) (*ChallengeReply, error) {
	return decodeChallengeReply(data)
}
