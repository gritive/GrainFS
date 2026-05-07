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
	Example: `  # Issue a new token, or print the existing one
  grainfs dashboard

  # Rotate the token (invalidate previous, issue a new one)
  grainfs dashboard --rotate`,
	RunE: runDashboard,
}

func init() {
	dashboardCmd.Flags().Bool("rotate", false, "rotate the dashboard auth token")
	dashboardCmd.Flags().String("format", "text", "Output format: text or json")
	registerAdminEndpointFlag(dashboardCmd)
	registerAdminTimeoutFlag(dashboardCmd)
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
	ctx, cancel := applyAdminTimeout(cmd.Context(), cmd)
	defer cancel()
	var resp dashboardResp
	if rotate {
		err = c.Post(ctx, "/v1/dashboard/token/rotate", nil, &resp)
	} else {
		err = c.Get(ctx, "/v1/dashboard/token", &resp)
	}
	if err != nil {
		return err
	}
	if jsonOut(cmd) {
		return printJSON(cmd, resp)
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
		fmt.Fprintln(os.Stderr, "         For remote access, set --public-url to the operator-facing hostname.")
		fmt.Fprintln(os.Stderr)
	}
	if !rotate {
		fmt.Println("Open the URL above in your browser. The token is passed in the URL fragment so it never appears in server logs.")
	}
	return nil
}
