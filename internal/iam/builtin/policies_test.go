package builtin

import (
	"context"
	"testing"

	"github.com/gritive/GrainFS/internal/iam/policy"
	"github.com/gritive/GrainFS/internal/iam/policystore"
)

func TestBuiltins_AllFourSeeded(t *testing.T) {
	ps := policystore.NewInMemoryStore()
	if err := SeedAll(context.Background(), ps); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"readonly", "readwrite", "writeonly", "bucket-admin"} {
		raw, err := ps.GetRaw(context.Background(), name)
		if err != nil {
			t.Errorf("missing builtin %q: %v", name, err)
			continue
		}
		if _, err := policy.Parse(raw); err != nil {
			t.Errorf("builtin %q does not parse: %v", name, err)
		}
		if !ps.IsBuiltin(name) {
			t.Errorf("builtin %q not marked builtin", name)
		}
	}
}

func TestBuiltins_Readonly(t *testing.T) {
	ps := policystore.NewInMemoryStore()
	if err := SeedAll(context.Background(), ps); err != nil {
		t.Fatal(err)
	}
	raw, _ := ps.GetRaw(context.Background(), "readonly")
	doc, err := policy.Parse(raw)
	if err != nil {
		t.Fatalf("parse readonly: %v", err)
	}
	in := policy.EvalInput{
		PrincipalPolicies: []*policy.Document{doc},
		Principal:         "sa-1",
		Ctx:               policy.RequestContext{Action: "s3:GetObject", Resource: "arn:aws:s3:::any/x"},
	}
	if policy.Evaluate(in).Decision != policy.DecisionAllow {
		t.Fatal("readonly should Allow s3:GetObject")
	}
	in.Ctx.Action = "s3:PutObject"
	if policy.Evaluate(in).Decision != policy.DecisionDeny {
		t.Fatal("readonly should Deny s3:PutObject")
	}
}

func TestBuiltins_BucketAdmin_ExcludesAdminUDSActions(t *testing.T) {
	ps := policystore.NewInMemoryStore()
	if err := SeedAll(context.Background(), ps); err != nil {
		t.Fatal(err)
	}
	raw, _ := ps.GetRaw(context.Background(), "bucket-admin")
	doc, _ := policy.Parse(raw)
	for _, a := range []string{
		"s3:CreateBucket", "s3:DeleteBucket",
		"s3:PutBucketPolicy", "s3:DeleteBucketPolicy",
	} {
		in := policy.EvalInput{
			PrincipalPolicies: []*policy.Document{doc},
			Principal:         "sa-1",
			Ctx:               policy.RequestContext{Action: a, Resource: "arn:aws:s3:::b"},
		}
		if policy.Evaluate(in).Decision == policy.DecisionAllow {
			t.Errorf("bucket-admin should NOT Allow %s (Decision #8: admin-UDS-only)", a)
		}
	}
}

func TestBuiltins_Writeonly_ListBucketDenied(t *testing.T) {
	ps := policystore.NewInMemoryStore()
	if err := SeedAll(context.Background(), ps); err != nil {
		t.Fatal(err)
	}
	raw, _ := ps.GetRaw(context.Background(), "writeonly")
	doc, _ := policy.Parse(raw)
	in := policy.EvalInput{
		PrincipalPolicies: []*policy.Document{doc},
		Principal:         "sa-1",
		Ctx:               policy.RequestContext{Action: "s3:ListBucket", Resource: "arn:aws:s3:::b"},
	}
	if policy.Evaluate(in).Decision != policy.DecisionDeny {
		t.Fatal("writeonly should Deny s3:ListBucket")
	}
}

func TestIsBuiltinName(t *testing.T) {
	if !IsBuiltinName("readonly") {
		t.Fatal("readonly should be builtin")
	}
	if IsBuiltinName("nope") {
		t.Fatal("'nope' should not be builtin")
	}
}
