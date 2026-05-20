package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

// TestCLI_GroupCreate_RoundTrip: PUT /v1/iam/group/data-team returns 204; CLI exits 0.
func TestCLI_GroupCreate_RoundTrip(t *testing.T) {
	const groupName = "data-team"
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iam/group/"+groupName, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			_, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	sock := startFakeAdminUDS(t, mux)

	var out bytes.Buffer
	rootCmd.SetArgs([]string{"iam", "--endpoint", sock, "group", "create", groupName})
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&out)
	rootCmd.SetContext(context.Background())
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("group create: %v\noutput: %s", err, out.String())
	}
	if s := out.String(); !strings.Contains(s, groupName) {
		t.Errorf("output missing group name %q:\n%s", groupName, s)
	}
}

// TestCLI_GroupDelete: DELETE /v1/iam/group/data-team returns 204; CLI exits 0.
func TestCLI_GroupDelete(t *testing.T) {
	const groupName = "data-team"
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/iam/group/"+groupName, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	sock := startFakeAdminUDS(t, mux)

	var out bytes.Buffer
	rootCmd.SetArgs([]string{"iam", "--endpoint", sock, "group", "delete", groupName})
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&out)
	rootCmd.SetContext(context.Background())
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("group delete: %v\noutput: %s", err, out.String())
	}
}

// TestCLI_GroupMemberAdd_Remove: PUT and DELETE /v1/iam/group/data-team/member/sa-alice
func TestCLI_GroupMemberAdd_Remove(t *testing.T) {
	const groupName = "data-team"
	const saID = "sa-alice"
	path := "/v1/iam/group/" + groupName + "/member/" + saID

	var gotMethod string
	mux := http.NewServeMux()
	mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		switch r.Method {
		case http.MethodPut, http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	sock := startFakeAdminUDS(t, mux)

	// member add
	var out bytes.Buffer
	rootCmd.SetArgs([]string{"iam", "--endpoint", sock, "group", "member", "add", groupName, saID})
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&out)
	rootCmd.SetContext(context.Background())
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("member add: %v\noutput: %s", err, out.String())
	}
	if gotMethod != http.MethodPut {
		t.Errorf("member add: expected PUT, got %s", gotMethod)
	}

	// member remove
	out.Reset()
	rootCmd.SetArgs([]string{"iam", "--endpoint", sock, "group", "member", "remove", groupName, saID})
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&out)
	rootCmd.SetContext(context.Background())
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("member remove: %v\noutput: %s", err, out.String())
	}
	if gotMethod != http.MethodDelete {
		t.Errorf("member remove: expected DELETE, got %s", gotMethod)
	}
}

// TestCLI_GroupPolicyAttach_Detach: PUT and DELETE /v1/iam/group/data-team/policy/readonly
func TestCLI_GroupPolicyAttach_Detach(t *testing.T) {
	const groupName = "data-team"
	const policyName = "readonly"
	path := "/v1/iam/group/" + groupName + "/policy/" + policyName

	var gotMethod string
	mux := http.NewServeMux()
	mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		switch r.Method {
		case http.MethodPut, http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	sock := startFakeAdminUDS(t, mux)

	// policy attach
	var out bytes.Buffer
	rootCmd.SetArgs([]string{"iam", "--endpoint", sock, "group", "policy", "attach", groupName, policyName})
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&out)
	rootCmd.SetContext(context.Background())
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("policy attach: %v\noutput: %s", err, out.String())
	}
	if gotMethod != http.MethodPut {
		t.Errorf("policy attach: expected PUT, got %s", gotMethod)
	}

	// policy detach
	out.Reset()
	rootCmd.SetArgs([]string{"iam", "--endpoint", sock, "group", "policy", "detach", groupName, policyName})
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&out)
	rootCmd.SetContext(context.Background())
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("policy detach: %v\noutput: %s", err, out.String())
	}
	if gotMethod != http.MethodDelete {
		t.Errorf("policy detach: expected DELETE, got %s", gotMethod)
	}
}
