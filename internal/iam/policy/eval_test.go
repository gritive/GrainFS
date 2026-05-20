package policy

import "testing"

type evalCase struct {
	name             string
	principalPols    []string
	resourcePol      string
	action, resource string
	sourceIP, prefix string
	allowAnonBucket  bool
	want             Decision
}

func TestEvaluate_Matrix(t *testing.T) {
	cases := []evalCase{
		{
			name:          "explicit Allow on action+resource",
			principalPols: []string{`{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::a/*"}]}`},
			action:        "s3:GetObject", resource: "arn:aws:s3:::a/x",
			want: DecisionAllow,
		},
		{
			name:          "implicit Deny when no statement matches",
			principalPols: []string{`{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::a/*"}]}`},
			action:        "s3:PutObject", resource: "arn:aws:s3:::a/x",
			want: DecisionDeny,
		},
		{
			name: "explicit Deny overrides Allow (F#9)",
			principalPols: []string{
				`{"Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`,
				`{"Statement":[{"Effect":"Deny","Action":"s3:GetObject","Resource":"arn:aws:s3:::secret/*"}]}`,
			},
			action: "s3:GetObject", resource: "arn:aws:s3:::secret/k",
			want: DecisionDeny,
		},
		{
			name:          "bucket policy combines with SA policy (union)",
			principalPols: nil,
			resourcePol:   `{"Statement":[{"Effect":"Allow","Principal":{"AWS":["sa-1"]},"Action":"s3:GetObject","Resource":"arn:aws:s3:::a/*"}]}`,
			action:        "s3:GetObject", resource: "arn:aws:s3:::a/x",
			want: DecisionAllow,
		},
		{
			name:        "Principal:* on bucket policy ignored without allow-anon flag (F#11)",
			resourcePol: `{"Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::p/*"}]}`,
			action:      "s3:GetObject", resource: "arn:aws:s3:::p/x",
			allowAnonBucket: false,
			want:            DecisionDeny,
		},
		{
			name:        "Principal:* honored when allow-anon flag is on",
			resourcePol: `{"Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::p/*"}]}`,
			action:      "s3:GetObject", resource: "arn:aws:s3:::p/x",
			allowAnonBucket: true,
			want:            DecisionAllow,
		},
		{
			// Security regression test: the Named-form wildcard MUST honor the same
			// AllowAnonBucket gate as the top-level Star form. Previously,
			// {"Principal":{"AWS":["*"]}} bypassed the gate by hitting the
			// `v == "*"` branch in principalMatches without consulting allowAnon.
			name:        "Principal:{AWS:[*]} on bucket policy ignored without allow-anon flag",
			resourcePol: `{"Statement":[{"Effect":"Allow","Principal":{"AWS":["*"]},"Action":"s3:GetObject","Resource":"arn:aws:s3:::p/*"}]}`,
			action:      "s3:GetObject", resource: "arn:aws:s3:::p/x",
			allowAnonBucket: false,
			want:            DecisionDeny,
		},
		{
			name:        "Principal:{AWS:[*]} honored when allow-anon flag is on",
			resourcePol: `{"Statement":[{"Effect":"Allow","Principal":{"AWS":["*"]},"Action":"s3:GetObject","Resource":"arn:aws:s3:::p/*"}]}`,
			action:      "s3:GetObject", resource: "arn:aws:s3:::p/x",
			allowAnonBucket: true,
			want:            DecisionAllow,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var pp []*Document
			for _, raw := range tc.principalPols {
				doc, err := Parse([]byte(raw))
				if err != nil {
					t.Fatalf("parse principal: %v", err)
				}
				pp = append(pp, doc)
			}
			var rp *Document
			if tc.resourcePol != "" {
				d, err := Parse([]byte(tc.resourcePol))
				if err != nil {
					t.Fatalf("parse resource: %v", err)
				}
				rp = d
			}
			ctx := RequestContext{
				Action: tc.action, Resource: tc.resource,
				SourceIP: tc.sourceIP, Prefix: tc.prefix,
			}
			d := Evaluate(EvalInput{
				PrincipalPolicies: pp,
				ResourcePolicy:    rp,
				Principal:         "sa-1",
				Ctx:               ctx,
				AllowAnonBucket:   tc.allowAnonBucket,
			})
			if d.Decision != tc.want {
				t.Fatalf("decision = %v, want %v (reason: %s)", d.Decision, tc.want, d.Reason)
			}
		})
	}
}

// TestEvaluate_MatchedPolicy verifies that EvalResult.MatchedPolicy is
// populated from PrincipalPolicyNames / ResourcePolicyBucket. T51' §6.
func TestEvaluate_MatchedPolicy(t *testing.T) {
	readonly, err := Parse([]byte(`{"Statement":[{"Sid":"AllowGet","Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::a/*"}]}`))
	if err != nil {
		t.Fatalf("parse readonly: %v", err)
	}
	denyAll, err := Parse([]byte(`{"Statement":[{"Sid":"DenyPut","Effect":"Deny","Action":"s3:PutObject","Resource":"*"}]}`))
	if err != nil {
		t.Fatalf("parse denyAll: %v", err)
	}
	bucketPol, err := Parse([]byte(`{"Statement":[{"Sid":"BucketAllow","Effect":"Allow","Principal":{"AWS":["sa-1"]},"Action":"s3:HeadObject","Resource":"arn:aws:s3:::a/*"}]}`))
	if err != nil {
		t.Fatalf("parse bucketPol: %v", err)
	}

	cases := []struct {
		name         string
		in           EvalInput
		ctx          RequestContext
		wantDecision Decision
		wantPolicy   string
		wantSid      string
	}{
		{
			name: "principal allow tags policy name",
			in: EvalInput{
				PrincipalPolicies:    []*Document{readonly},
				PrincipalPolicyNames: []string{"readonly"},
				Principal:            "sa-1",
			},
			ctx:          RequestContext{Action: "s3:GetObject", Resource: "arn:aws:s3:::a/x"},
			wantDecision: DecisionAllow,
			wantPolicy:   "readonly",
			wantSid:      "AllowGet",
		},
		{
			name: "principal explicit deny tags policy name",
			in: EvalInput{
				PrincipalPolicies:    []*Document{readonly, denyAll},
				PrincipalPolicyNames: []string{"readonly", "deny-puts"},
				Principal:            "sa-1",
			},
			ctx:          RequestContext{Action: "s3:PutObject", Resource: "arn:aws:s3:::a/x"},
			wantDecision: DecisionDeny,
			wantPolicy:   "deny-puts",
			wantSid:      "DenyPut",
		},
		{
			name: "bucket policy allow tags bucket:<name>",
			in: EvalInput{
				ResourcePolicy:       bucketPol,
				ResourcePolicyBucket: "a",
				Principal:            "sa-1",
			},
			ctx:          RequestContext{Action: "s3:HeadObject", Resource: "arn:aws:s3:::a/x"},
			wantDecision: DecisionAllow,
			wantPolicy:   "bucket:a",
			wantSid:      "BucketAllow",
		},
		{
			name: "implicit deny leaves matched policy empty",
			in: EvalInput{
				PrincipalPolicies:    []*Document{readonly},
				PrincipalPolicyNames: []string{"readonly"},
				Principal:            "sa-1",
			},
			ctx:          RequestContext{Action: "s3:DeleteObject", Resource: "arn:aws:s3:::a/x"},
			wantDecision: DecisionDeny,
			wantPolicy:   "",
			wantSid:      "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.in.Ctx = tc.ctx
			got := Evaluate(tc.in)
			if got.Decision != tc.wantDecision {
				t.Fatalf("decision = %v want %v (reason: %s)", got.Decision, tc.wantDecision, got.Reason)
			}
			if got.MatchedPolicy != tc.wantPolicy {
				t.Fatalf("MatchedPolicy = %q want %q", got.MatchedPolicy, tc.wantPolicy)
			}
			if got.MatchedSid != tc.wantSid {
				t.Fatalf("MatchedSid = %q want %q", got.MatchedSid, tc.wantSid)
			}
		})
	}
}
