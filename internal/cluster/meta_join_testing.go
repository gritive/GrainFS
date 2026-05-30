// Test-only helpers exported for cross-package tests (serveruntime drives
// the production wiring path and needs to encode/decode JoinRequest /
// JoinReply without re-implementing the flatbuffer layouts). Keep ForTest
// suffix so they cannot be mistaken for production API.
package cluster

func EncodeJoinRequestForTest(req JoinRequest) ([]byte, error) {
	return encodeJoinRequest(req)
}

func EncodeJoinReplyForTest(reply JoinReply) ([]byte, error) {
	return encodeJoinReply(reply)
}

func DecodeJoinReplyForTest(data []byte) (*JoinReply, error) {
	return decodeJoinReply(data)
}
