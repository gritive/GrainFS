package e2e

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/hugelgupf/p9/p9"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/stretchr/testify/require"
)

// P9 Content-Type preservation exercises the cross-protocol invariant
// (NFS§B T10):
//   - When S3 PUT stamps a Content-Type and a subsequent mount-protocol write
//     overwrites the object, HeadObject must still return the original
//     Content-Type (preservation via HeadObject→PutObject reuse).
//   - When a mount-protocol write creates a brand new key, HeadObject returns
//     application/octet-stream (the mount-default).
//
// This is the e2e equivalent of internal/p9server/content_type_test.go on
// real wire (9P over TCP + S3 over HTTP). The NFSv4 wire-equivalent is
// deferred to tests/nfs4_colima/ (real kernel mount).
//
// SingleNode only — both cases require anon attach to /default, which is
// not possible on bootstrapped Phase 2 mrCluster (the 9P attach gate does not
// honor S3's default-bucket implicit-anon allow path; reported as
// F-§B-9P-anon-attach-phase2). Cluster3Node coverage requires either a Phase 0
// mrCluster fixture (does not exist) or 9P attach-gate parity with S3.
var _ = ginkgo.Describe("P9 Content-Type preservation", ginkgo.Label("p9", "content-type"), func() {
	describeP9ContentTypeContext("SingleNode", func(tb testing.TB) *p9Target {
		return newSingleNodeP9Target(tb)
	})
})

func describeP9ContentTypeContext(name string, factory func(testing.TB) *p9Target) {
	ginkgo.Context(name, ginkgo.Ordered, func() {
		var tgt *p9Target

		ginkgo.BeforeAll(func() {
			tgt = factory(ginkgo.GinkgoTB())
		})

		// Both cases write to /default which carries implicit anon Allow on every
		// phase. No bootstrap needed.
		bucket := "default"

		// S3PutImagePNG_NFSWrite_S3GetPreserved: S3 PUT image/png, 9P overwrite,
		// S3 HEAD returns image/png.
		ginkgo.It("preserves S3 Content-Type across 9P overwrite (S3PutImagePNG_NFSWrite_S3GetPreserved)", func() {
			t := ginkgo.GinkgoTB()
			key := "preserve-" + sanitizeForBucket(tgt.name) + ".png"
			objURL := tgt.s3URL(0) + "/" + bucket + "/" + key

			// S3 PUT with explicit Content-Type=image/png (anon, raw HTTP — Phase 0
			// allows; Phase 2 /default retains implicit anon allow).
			req, err := http.NewRequest(http.MethodPut, objURL,
				bytes.NewReader([]byte("\x89PNG\r\n\x1a\nseed")))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "image/png")
			resp, err := anonHTTPClient().Do(req)
			require.NoError(t, err, "S3 PUT seed image/png")
			_ = resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode,
				"S3 anon PUT to /default must succeed (target=%s)", tgt.name)

			ginkgo.DeferCleanup(func() { anonDelete(objURL) })

			// Overwrite via 9P: Attach /default, Walk to key, Open WriteOnly,
			// WriteAt.
			root, cli, err := attachP9(t, tgt, 0, bucket)
			require.NoError(t, err)
			defer cli.Close()
			defer closeP9File(root)

			_, file, err := root.Walk([]string{key})
			require.NoError(t, err, "9P Walk to existing key")
			defer closeP9File(file)

			_, _, err = file.Open(p9.WriteOnly)
			require.NoError(t, err, "9P Open WriteOnly")

			newBody := []byte("9P-overwrite-body")
			n, err := file.WriteAt(newBody, 0)
			require.NoError(t, err, "9P WriteAt")
			require.Equal(t, len(newBody), n)
			require.NoError(t, file.FSync(), "9P FSync after write")

			// Force the 9P-side fid release so the write commits before HEAD.
			closeP9File(file)

			// S3 HEAD must still report image/png.
			headReq, err := http.NewRequest(http.MethodHead, objURL, nil)
			require.NoError(t, err)
			headResp, err := anonHTTPClient().Do(headReq)
			require.NoError(t, err, "S3 HEAD after 9P overwrite")
			_ = headResp.Body.Close()
			require.Equal(t, http.StatusOK, headResp.StatusCode,
				"S3 anon HEAD on /default must succeed")
			gotCT := headResp.Header.Get("Content-Type")
			gomega.Expect(strings.ToLower(gotCT)).To(gomega.Equal("image/png"),
				"Content-Type must be preserved across 9P overwrite (got %q)", gotCT)
		})

		// NFSWriteNewFile_DefaultOctetStream: 9P Create on a new key → S3 HEAD
		// returns application/octet-stream.
		ginkgo.It("defaults new 9P-created files to application/octet-stream (NFSWriteNewFile_DefaultOctetStream)", func() {
			t := ginkgo.GinkgoTB()
			key := "newfile-" + sanitizeForBucket(tgt.name) + ".bin"
			objURL := tgt.s3URL(0) + "/" + bucket + "/" + key
			ginkgo.DeferCleanup(func() { anonDelete(objURL) })

			root, cli, err := attachP9(t, tgt, 0, bucket)
			require.NoError(t, err)
			defer cli.Close()
			defer closeP9File(root)

			newFile, _, _, err := root.Create(key, p9.WriteOnly, 0644, 0, 0)
			require.NoError(t, err, "9P Create new key")
			defer closeP9File(newFile)

			body := []byte("brand-new-mount-write")
			n, err := newFile.WriteAt(body, 0)
			require.NoError(t, err)
			require.Equal(t, len(body), n)
			require.NoError(t, newFile.FSync())
			closeP9File(newFile)

			// S3 HEAD on the new key.
			headReq, err := http.NewRequest(http.MethodHead, objURL, nil)
			require.NoError(t, err)
			headResp, err := anonHTTPClient().Do(headReq)
			require.NoError(t, err, "S3 HEAD on new 9P-created key")
			_ = headResp.Body.Close()
			require.Equal(t, http.StatusOK, headResp.StatusCode)
			ct := strings.ToLower(headResp.Header.Get("Content-Type"))
			gomega.Expect(ct).To(gomega.Equal("application/octet-stream"),
				"new 9P file Content-Type must default to application/octet-stream (got %q)", ct)
		})
	})
}

// anonHTTPClient builds an *http.Client with no keep-alive and no credentials.
// Use for anon (no Authorization header) S3 ops against /default in both
// Phase 0 (anon-enabled=true) and Phase 2 (default-bucket implicit anon allow).
func anonHTTPClient() *http.Client {
	return e2eNoKeepAliveHTTPClient(0)
}

// anonDelete best-effort DELETEs the object via raw HTTP. Ignores errors.
func anonDelete(objURL string) {
	req, err := http.NewRequest(http.MethodDelete, objURL, nil)
	if err != nil {
		return
	}
	resp, err := anonHTTPClient().Do(req)
	if err != nil {
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}
