package server

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/internal/storage"
)

// setupECAuthServer starts an in-process HTTP server backed by LocalBackend
// (which now implements storage.ACLSetter) and returns the base URL, a
// signing helper, and the backend. LocalBackend is sufficient because ACL
// serialization correctness is covered by cluster/apply_test.go; here we
// test the HTTP layer.
func setupECAuthServer(t interface {
	serverTestTB
	serverCleanupTB
	TempDir() string
	Cleanup(func())
}) (baseURL string, sign func(*http.Request), backend *storage.LocalBackend) {
	t.Helper()
	dir := t.TempDir()
	var err error
	backend, err = storage.NewLocalBackend(dir)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(backend.Close)

	const (
		accessKey = "testkey"
		secretKey = "testsecret"
	)
	creds := []s3auth.Credentials{{AccessKey: accessKey, SecretKey: secretKey}}
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	srv := New(addr, backend, WithAuth(creds))
	DeferCleanup(shutdownTestServer, t, srv)
	go srv.Run() //nolint:errcheck
	for i := 0; i < 50; i++ {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			conn.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	base := "http://" + addr
	signFn := func(req *http.Request) {
		req.Host = req.URL.Host
		s3auth.SignRequest(req, accessKey, secretKey, "us-east-1")
	}
	return base, signFn, backend
}

var _ = Describe("ACL integration", func() {
	var (
		base    string
		sign    func(*http.Request)
		backend *storage.LocalBackend
	)

	BeforeEach(func() {
		base, sign, backend = setupECAuthServer(GinkgoT())
		mustCreateBucket(GinkgoT(), backend, "testbucket")
	})

	It("allows anonymous GET for public-read objects", func() {
		body := []byte("hello world")
		req, err := http.NewRequest(http.MethodPut, base+"/testbucket/public.txt", bytes.NewReader(body))
		Expect(err).NotTo(HaveOccurred())
		req.Header.Set("x-amz-acl", "public-read")
		sign(req)
		resp, err := http.DefaultClient.Do(req)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Body.Close()).To(Succeed())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))

		req, err = http.NewRequest(http.MethodGet, base+"/testbucket/public.txt", nil)
		Expect(err).NotTo(HaveOccurred())
		resp, err = http.DefaultClient.Do(req)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(resp.Body.Close)
		Expect(resp.StatusCode).To(Equal(http.StatusOK))

		data, err := io.ReadAll(resp.Body)
		Expect(err).NotTo(HaveOccurred())
		Expect(data).To(Equal(body))
	})

	It("denies anonymous GET for private objects", func() {
		req, err := http.NewRequest(http.MethodPut, base+"/testbucket/private.txt", bytes.NewReader([]byte("secret")))
		Expect(err).NotTo(HaveOccurred())
		sign(req)
		resp, err := http.DefaultClient.Do(req)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Body.Close()).To(Succeed())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))

		req, err = http.NewRequest(http.MethodGet, base+"/testbucket/private.txt", nil)
		Expect(err).NotTo(HaveOccurred())
		resp, err = http.DefaultClient.Do(req)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Body.Close()).To(Succeed())
		Expect(resp.StatusCode).To(Equal(http.StatusForbidden))
	})

	It("allows authenticated GET for public-read objects", func() {
		body := []byte("public data")
		req, err := http.NewRequest(http.MethodPut, base+"/testbucket/pub.txt", bytes.NewReader(body))
		Expect(err).NotTo(HaveOccurred())
		req.Header.Set("x-amz-acl", "public-read")
		sign(req)
		resp, err := http.DefaultClient.Do(req)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Body.Close()).To(Succeed())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))

		req, err = http.NewRequest(http.MethodGet, base+"/testbucket/pub.txt", nil)
		Expect(err).NotTo(HaveOccurred())
		sign(req)
		resp, err = http.DefaultClient.Do(req)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(resp.Body.Close)
		Expect(resp.StatusCode).To(Equal(http.StatusOK))
	})
})
