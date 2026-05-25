//go:build lifecycle

package lifecycle_test

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("docs_runner", func() {
	It("extracts blocks by phase annotation", func() {
		md := "# fixture\n\n" +
			"<!-- lifecycle-test:phase=0 -->\n" +
			"```bash\n" +
			"echo hello-0\n" +
			"```\n\n" +
			"other text\n\n" +
			"<!-- lifecycle-test:phase=2 -->\n" +
			"```bash\n" +
			"echo hello-2\n" +
			"```\n"
		blocks, err := ExtractBlocks(md, "0")
		Expect(err).NotTo(HaveOccurred())
		Expect(blocks).To(HaveLen(1))
		Expect(blocks[0]).To(ContainSubstring("echo hello-0"))
		Expect(blocks[0]).NotTo(ContainSubstring("hello-2"))
	})

	It("substitutes ${VAR} from environment", func() {
		os.Setenv("GRAINFS_TEST_X", "world")
		DeferCleanup(func() { os.Unsetenv("GRAINFS_TEST_X") })
		got := Envsubst("echo ${GRAINFS_TEST_X}")
		Expect(got).To(Equal("echo world"))
	})

	It("runs a substituted block under bash", func() {
		tmp := GinkgoT().TempDir()
		marker := filepath.Join(tmp, "marker")
		os.Setenv("LIFECYCLE_MARKER", marker)
		DeferCleanup(func() { os.Unsetenv("LIFECYCLE_MARKER") })

		block := `echo hi > ${LIFECYCLE_MARKER}`
		Expect(RunBlock(Envsubst(block))).To(Succeed())

		b, err := os.ReadFile(marker)
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.TrimSpace(string(b))).To(Equal("hi"))
	})
})

var annotationRe = regexp.MustCompile(`^<!--\s*lifecycle-test:phase=([^\s:]+)(?::[^\s]+)?\s*-->\s*$`)

// ExtractBlocks scans markdown for `<!-- lifecycle-test:phase=N -->`
// annotations followed by a fenced bash block. It returns the bodies of
// blocks whose phase matches `phase`. Sub-tags after a colon
// (`phase=3:tls`) match `phase == "3"`.
func ExtractBlocks(markdown, phase string) ([]string, error) {
	var out []string
	sc := bufio.NewScanner(strings.NewReader(markdown))
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	state := "scan" // scan → expectFence → collect
	var match bool
	var body strings.Builder
	for sc.Scan() {
		line := sc.Text()
		switch state {
		case "scan":
			m := annotationRe.FindStringSubmatch(line)
			if m == nil {
				continue
			}
			match = m[1] == phase
			state = "expectFence"
		case "expectFence":
			if strings.HasPrefix(line, "```") {
				if match {
					body.Reset()
				}
				state = "collect"
			} else if strings.TrimSpace(line) != "" {
				// annotation followed by non-fence non-blank — abort
				state = "scan"
			}
		case "collect":
			if strings.HasPrefix(line, "```") {
				if match {
					out = append(out, body.String())
				}
				state = "scan"
				match = false
				continue
			}
			if match {
				body.WriteString(line)
				body.WriteString("\n")
			}
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("scan markdown: %w", err)
	}
	return out, nil
}

// Envsubst replaces `${VAR}` references with os.Getenv(VAR). Missing
// vars produce empty strings (matches the GNU envsubst default).
var varRe = regexp.MustCompile(`\$\{([A-Z0-9_]+)\}`)

func Envsubst(in string) string {
	return varRe.ReplaceAllStringFunc(in, func(match string) string {
		name := match[2 : len(match)-1]
		return os.Getenv(name)
	})
}

// RunBlock executes `block` under `bash -e -u -o pipefail -c`. Stdout
// and stderr are wired to the surrounding test's stderr for debugging.
func RunBlock(block string) error {
	cmd := exec.Command("bash", "-e", "-u", "-o", "pipefail", "-c", block)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
