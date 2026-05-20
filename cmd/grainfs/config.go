package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"
)

// stdoutIsTerminal returns true when os.Stdout is connected to a real terminal.
// Under `go test`, stdout is a pipe → returns false (JSON path).
// Uses the portable os.ModeCharDevice check — no external dependency needed.
func stdoutIsTerminal() bool {
	fi, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}

// configCmd returns the `grainfs config` command tree.
func configCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Manage cluster-wide configuration",
	}
	cmd.PersistentFlags().String("endpoint", "",
		"admin Unix socket path (overrides GRAINFS_ADMIN_SOCKET env var)")
	cmd.PersistentFlags().Bool("json", false, "output raw JSON")

	cmd.AddCommand(
		configSetCmd(),
		configGetCmd(),
		configUnsetCmd(),
		configListCmd(),
	)
	return cmd
}

func configSetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "set <key> <value>",
		Short: "Set a cluster-wide config key",
		Example: `  grainfs config set audit.deny-only true
  grainfs config set trusted-proxy.cidr 10.0.0.0/8`,
		Args: cobra.ExactArgs(2),
		RunE: func(c *cobra.Command, args []string) error {
			sock, err := adminEndpointFromCmd(c)
			if err != nil {
				return err
			}
			key, value := args[0], args[1]
			_, err = iamRequest(c.Context(), sock, "PUT",
				"/v1/config/"+url.PathEscape(key),
				map[string]string{"value": value})
			if err != nil {
				return err
			}
			asJSON, _ := c.Flags().GetBool("json")
			if asJSON || !stdoutIsTerminal() {
				fmt.Fprintf(c.OutOrStdout(), "{\"key\":%q,\"value\":%q}\n", key, value)
				return nil
			}
			fmt.Fprintf(c.OutOrStdout(), "Set %s = %s\n", key, value)
			return nil
		},
	}
}

func configGetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get <key>",
		Short: "Get the current value of a cluster-wide config key",
		Example: `  grainfs config get audit.deny-only
  grainfs config --json get audit.deny-only`,
		Args: cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			sock, err := adminEndpointFromCmd(c)
			if err != nil {
				return err
			}
			out, err := iamRequest(c.Context(), sock, "GET",
				"/v1/config/"+url.PathEscape(args[0]), nil)
			if err != nil {
				return err
			}
			asJSON, _ := c.Flags().GetBool("json")
			if asJSON || !stdoutIsTerminal() {
				fmt.Fprintln(c.OutOrStdout(), string(out))
				return nil
			}
			var entry struct {
				Key         string `json:"key"`
				Value       string `json:"value"`
				Kind        string `json:"kind"`
				Default     string `json:"default"`
				Set         bool   `json:"set"`
				Description string `json:"description"`
			}
			if err := json.Unmarshal(out, &entry); err != nil {
				return fmt.Errorf("parse response: %w", err)
			}
			tw := tabwriter.NewWriter(c.OutOrStdout(), 0, 0, 2, ' ', 0)
			fmt.Fprintf(tw, "key:\t%s\n", entry.Key)
			fmt.Fprintf(tw, "value:\t%s\n", entry.Value)
			fmt.Fprintf(tw, "kind:\t%s\n", entry.Kind)
			fmt.Fprintf(tw, "default:\t%s\n", entry.Default)
			fmt.Fprintf(tw, "set:\t%v\n", entry.Set)
			if entry.Description != "" {
				fmt.Fprintf(tw, "description:\t%s\n", entry.Description)
			}
			return tw.Flush()
		},
	}
}

func configUnsetCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "unset <key>",
		Short:   "Revert a cluster-wide config key to its default value",
		Example: `  grainfs config unset audit.deny-only`,
		Args:    cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			sock, err := adminEndpointFromCmd(c)
			if err != nil {
				return err
			}
			_, err = iamRequest(c.Context(), sock, "DELETE",
				"/v1/config/"+url.PathEscape(args[0]), nil)
			if err != nil {
				return err
			}
			asJSON, _ := c.Flags().GetBool("json")
			if asJSON || !stdoutIsTerminal() {
				fmt.Fprintf(c.OutOrStdout(), "{\"key\":%q,\"unset\":true}\n", args[0])
				return nil
			}
			fmt.Fprintf(c.OutOrStdout(), "Unset %s (reverted to default)\n", args[0])
			return nil
		},
	}
}

func configListCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List cluster-wide config keys",
		Example: `  grainfs config list           # show only explicitly-set keys
  grainfs config list --all    # show full catalog with types and descriptions
  grainfs config --json list   # JSON output`,
		RunE: func(c *cobra.Command, args []string) error {
			sock, err := adminEndpointFromCmd(c)
			if err != nil {
				return err
			}
			all, _ := c.Flags().GetBool("all")
			path := "/v1/config"
			if all {
				path += "?all=true"
			}
			raw, err := iamRequest(c.Context(), sock, "GET", path, nil)
			if err != nil {
				return err
			}

			var entries []struct {
				Key         string `json:"key"`
				Value       string `json:"value"`
				Kind        string `json:"kind"`
				Default     string `json:"default"`
				Set         bool   `json:"set"`
				Description string `json:"description"`
			}
			if err := json.Unmarshal(raw, &entries); err != nil {
				return fmt.Errorf("parse response: %w", err)
			}

			// Without --all, only show set keys (filter client-side based on Set flag).
			if !all {
				filtered := entries[:0]
				for _, e := range entries {
					if e.Set {
						filtered = append(filtered, e)
					}
				}
				entries = filtered
			}

			asJSON, _ := c.Flags().GetBool("json")
			if asJSON || !stdoutIsTerminal() {
				out, _ := json.Marshal(entries)
				fmt.Fprintln(c.OutOrStdout(), string(out))
				return nil
			}

			if len(entries) == 0 {
				fmt.Fprintln(c.OutOrStdout(), "(no config keys set)")
				return nil
			}

			tw := tabwriter.NewWriter(c.OutOrStdout(), 0, 0, 2, ' ', 0)
			if all {
				fmt.Fprintln(tw, "KEY\tKIND\tVALUE\tDEFAULT\tSET\tDESCRIPTION")
				for _, e := range entries {
					desc := e.Description
					if desc == "" {
						desc = "-"
					}
					fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%v\t%s\n",
						e.Key, e.Kind, e.Value, e.Default, e.Set,
						strings.ReplaceAll(desc, "\t", " "))
				}
			} else {
				fmt.Fprintln(tw, "KEY\tKIND\tVALUE")
				for _, e := range entries {
					fmt.Fprintf(tw, "%s\t%s\t%s\n", e.Key, e.Kind, e.Value)
				}
			}
			return tw.Flush()
		},
	}
	cmd.Flags().Bool("all", false, "show full catalog including unset keys with type and description")
	return cmd
}

func init() {
	rootCmd.AddCommand(configCmd())
}
