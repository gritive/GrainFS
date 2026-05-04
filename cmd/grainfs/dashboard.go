package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var dashboardCmd = &cobra.Command{
	Use:   "dashboard",
	Short: "Open the GrainFS web dashboard (issues an auth token URL)",
	Long: `Issues a token-bearing URL for the dashboard. The browser receives the
token via the URL fragment and stores it in localStorage; subsequent fetches
include it as Authorization: Bearer. Use --rotate to invalidate the existing
token (existing browser sessions will see 401 and the operator runs this
command again to get a new URL).`,
	Example: `  # 토큰 발급 또는 기존 토큰 표시
  grainfs dashboard

  # 토큰 폐기 후 재발급
  grainfs dashboard --rotate`,
	RunE: runDashboard,
}

func init() {
	dashboardCmd.Flags().Bool("rotate", false, "rotate the dashboard auth token")
	dashboardCmd.Flags().String("endpoint", "", "admin endpoint (default: auto-discover)")
	dashboardCmd.Flags().String("data", "", "data directory for admin socket auto-discovery")
	dashboardCmd.Flags().Bool("json", false, "JSON output for scripting")
	rootCmd.AddCommand(dashboardCmd)
}

type dashboardResp struct {
	URL          string `json:"url"`
	Host         string `json:"host"`
	Token        string `json:"token"`
	Path         string `json:"path"`
	PublicURLSet bool   `json:"public_url_set"`
}

func runDashboard(cmd *cobra.Command, args []string) error {
	rotate, _ := cmd.Flags().GetBool("rotate")
	c, err := adminClientFromCmd(cmd)
	if err != nil {
		return err
	}
	var resp dashboardResp
	if rotate {
		err = c.post("/v1/dashboard/token/rotate", nil, &resp)
	} else {
		err = c.get("/v1/dashboard/token", &resp)
	}
	if err != nil {
		return err
	}
	if jsonOut(cmd) {
		return printJSON(resp)
	}
	if rotate {
		fmt.Println("Token rotated. Existing browser sessions will see 401 on next request.")
	} else {
		fmt.Println("Dashboard ready.")
	}
	fmt.Println()
	fmt.Printf("  URL:    %s\n", resp.URL)
	fmt.Printf("  Host:   %s\n", resp.Host)
	fmt.Printf("  Token:  %s (mode 0600, 32 bytes)\n", resp.Path)
	fmt.Printf("  Rotate: grainfs dashboard --rotate\n")
	fmt.Println()
	if !resp.PublicURLSet {
		fmt.Fprintln(os.Stderr, "warning: --public-url not set; using listen address.")
		fmt.Fprintln(os.Stderr, "         원격 접근 시 운영자 호스트명을 --public-url 로 지정하거나,")
		fmt.Fprintln(os.Stderr, "         grainfs.toml 의 public_url 에 설정.")
		fmt.Fprintln(os.Stderr)
	}
	if !rotate {
		fmt.Println("브라우저에서 위 URL 을 열어 사용하세요. 토큰은 fragment 로 전달되므로 서버 로그에 남지 않습니다.")
	}
	return nil
}
