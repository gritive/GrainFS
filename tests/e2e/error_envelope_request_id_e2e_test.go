// Error envelope request_id propagation e2e (auth-redesign §5 T42).
//
// Verifies that the X-GrainFS-Request-Id header set by WithRequestID (T41)
// is propagated INTO the response body for both S3 XML and Iceberg JSON
// error envelopes. Clients can correlate failures with server-side audit
// records without trusting only the response header.
//
// Coverage:
//   - S3 XML: unsigned GET → SigV4 rejection → <RequestId> embedded in body
//   - Iceberg JSON: unsigned /iceberg/v1/config → 401 → "request_id" embedded
//   - Generated-rid path: omit the incoming header, ensure the server-
//     generated rid appears in the body AND matches the response header.
package e2e

import (
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// TestErrorEnvelopeRequestIDE2E covers §5 T42 on single + cluster targets.
var _ = ginkgo.Describe("Error envelope request ID", func() {
	for _, tc := range []struct {
		name string
		mk   func() s3Target
	}{
		{name: "SingleNode", mk: newSingleNodeS3Target},
		{name: "Cluster4Node", mk: func() s3Target { return newSharedClusterS3Target(ginkgo.GinkgoTB()) }},
	} {
		tc := tc
		ginkgo.Context(tc.name, func() {
			var tgt s3Target

			ginkgo.BeforeEach(func() {
				tgt = tc.mk()
			})

			runErrorEnvelopeRequestIDCases(func() s3Target { return tgt })
		})
	}
})

func runErrorEnvelopeRequestIDCases(getTgt func() s3Target) {
	ginkgo.It("propagates an incoming request ID into S3 XML errors", func() {
		endpoint := getTgt().endpoint(0)
		const incoming = "rid-test-s3"
		// Unsigned GET on a non-existent bucket — SigV4 gate produces an XML
		// auth error envelope via writeXMLError. We don't care WHICH XML
		// error is returned; only that the rid was propagated into the body.
		req, err := http.NewRequest(http.MethodGet, endpoint+"/no-such-bucket-rid-test/", nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		req.Header.Set(reqIDHeader, incoming)

		resp, err := http.DefaultClient.Do(req)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() { _, _ = io.Copy(io.Discard, resp.Body); _ = resp.Body.Close() }()
		body, err := io.ReadAll(resp.Body)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		gomega.Expect(resp.StatusCode).To(gomega.BeNumerically(">=", 400), "expected an error response")
		gomega.Expect(resp.Header.Get(reqIDHeader)).To(gomega.Equal(incoming),
			"server must preserve client-supplied X-GrainFS-Request-Id")

		var env struct {
			XMLName   xml.Name `xml:"Error"`
			Code      string   `xml:"Code"`
			RequestID string   `xml:"RequestId"`
		}
		gomega.Expect(xml.Unmarshal(body, &env)).To(gomega.Succeed(), "body must be S3 XML error: %s", body)
		gomega.Expect(env.RequestID).To(gomega.Equal(incoming),
			"S3 XML <RequestId> must echo X-GrainFS-Request-Id; body=%s", body)
	})

	ginkgo.It("propagates a generated request ID into S3 XML errors", func() {
		endpoint := getTgt().endpoint(0)
		// Omit the incoming header — server generates a UUIDv7; the rid in the
		// response body must match the rid in the response header.
		req, err := http.NewRequest(http.MethodGet, endpoint+"/no-such-bucket-rid-gen/", nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		resp, err := http.DefaultClient.Do(req)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() { _, _ = io.Copy(io.Discard, resp.Body); _ = resp.Body.Close() }()
		body, err := io.ReadAll(resp.Body)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		hdrRid := resp.Header.Get(reqIDHeader)
		gomega.Expect(hdrRid).NotTo(gomega.BeEmpty(), "server must generate rid when absent")

		var env struct {
			XMLName   xml.Name `xml:"Error"`
			RequestID string   `xml:"RequestId"`
		}
		gomega.Expect(xml.Unmarshal(body, &env)).To(gomega.Succeed(), "body must be S3 XML error: %s", body)
		gomega.Expect(env.RequestID).To(gomega.Equal(hdrRid),
			"S3 XML <RequestId> must match X-GrainFS-Request-Id header (generated path)")
	})

	ginkgo.It("propagates an incoming request ID into Iceberg JSON errors", func() {
		endpoint := getTgt().endpoint(0)
		const incoming = "rid-test-iceberg"
		// Unsigned Iceberg request — SigV4 gate rejects → writeIcebergError
		// remaps to 401 JSON envelope.
		req, err := http.NewRequest(http.MethodGet,
			endpoint+"/iceberg/v1/config?warehouse=rid-test-warehouse", nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		req.Header.Set(reqIDHeader, incoming)

		resp, err := http.DefaultClient.Do(req)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() { _, _ = io.Copy(io.Discard, resp.Body); _ = resp.Body.Close() }()
		body, err := io.ReadAll(resp.Body)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		gomega.Expect(resp.StatusCode).To(gomega.BeNumerically(">=", 400), "expected an error response")
		gomega.Expect(resp.Header.Get(reqIDHeader)).To(gomega.Equal(incoming),
			"server must preserve client-supplied X-GrainFS-Request-Id")
		gomega.Expect(resp.Header.Get("Content-Type")).To(gomega.ContainSubstring("application/json"),
			"iceberg error envelope must be JSON")

		var env map[string]any
		gomega.Expect(json.Unmarshal(body, &env)).To(gomega.Succeed(),
			"body must be Iceberg JSON error: %s", body)
		gotRid, _ := env["request_id"].(string)
		gomega.Expect(gotRid).To(gomega.Equal(incoming),
			"Iceberg JSON request_id must echo X-GrainFS-Request-Id; body=%s", body)
	})

	ginkgo.It("propagates a generated request ID into Iceberg JSON errors", func() {
		endpoint := getTgt().endpoint(0)
		req, err := http.NewRequest(http.MethodGet,
			endpoint+"/iceberg/v1/config?warehouse=rid-test-warehouse-gen", nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		resp, err := http.DefaultClient.Do(req)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() { _, _ = io.Copy(io.Discard, resp.Body); _ = resp.Body.Close() }()
		body, err := io.ReadAll(resp.Body)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		hdrRid := resp.Header.Get(reqIDHeader)
		gomega.Expect(hdrRid).NotTo(gomega.BeEmpty(), "server must generate rid when absent")
		gomega.Expect(resp.Header.Get("Content-Type")).To(gomega.ContainSubstring("application/json"),
			"iceberg error envelope must be JSON")

		var env map[string]any
		gomega.Expect(json.Unmarshal(body, &env)).To(gomega.Succeed(),
			"body must be Iceberg JSON error: %s", body)
		gotRid, _ := env["request_id"].(string)
		gomega.Expect(gotRid).To(gomega.Equal(hdrRid),
			"Iceberg JSON request_id must match X-GrainFS-Request-Id header (generated path)")
	})
}
