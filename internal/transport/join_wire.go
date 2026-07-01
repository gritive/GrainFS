package transport

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// joinMaxFrame bounds a single length-prefixed field on the join wire.
const joinMaxFrame = 1 << 20

// JoinALPN is the single, isolated ALPN for the Zero-CA join transport. It must
// stay distinct from the cluster data/mux ALPNs so a join dial can never
// negotiate the data plane and vice-versa.
const JoinALPN = "grainfs-join-v1"

// certSPKI returns the SHA-256 of a certificate's SubjectPublicKeyInfo.
func certSPKI(cert tls.Certificate) ([32]byte, error) {
	var out [32]byte
	leaf := cert.Leaf
	if leaf == nil {
		if len(cert.Certificate) == 0 {
			return out, errors.New("certificate has no leaf DER")
		}
		parsed, err := x509.ParseCertificate(cert.Certificate[0])
		if err != nil {
			return out, fmt.Errorf("parse leaf: %w", err)
		}
		leaf = parsed
	}
	return sha256.Sum256(leaf.RawSubjectPublicKeyInfo), nil
}

// JoinPutField appends a length-prefixed field to buf. JoinPutField/JoinReadFields
// are a small length-prefixed codec reused for the on-disk invite-join resume
// record (serveruntime); the join WIRE itself is now HTTP-framed.
func JoinPutField(buf []byte, f []byte) []byte {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(f)))
	buf = append(buf, hdr[:]...)
	return append(buf, f...)
}

// JoinReadFields reads exactly n length-prefixed fields from r.
func JoinReadFields(r io.Reader, n int) ([][]byte, error) {
	out := make([][]byte, n)
	for i := 0; i < n; i++ {
		var hdr [4]byte
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return nil, err
		}
		sz := binary.BigEndian.Uint32(hdr[:])
		if sz > joinMaxFrame {
			return nil, fmt.Errorf("join field too large: %d", sz)
		}
		body := make([]byte, sz)
		if _, err := io.ReadFull(r, body); err != nil {
			return nil, err
		}
		out[i] = body
	}
	return out, nil
}
