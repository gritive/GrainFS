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
