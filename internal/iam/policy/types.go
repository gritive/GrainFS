package policy

type Effect string

const (
	EffectAllow Effect = "Allow"
	EffectDeny  Effect = "Deny"
)

type Document struct {
	Version   string      `json:"Version,omitempty"`
	Statement []Statement `json:"Statement"`
}

type Statement struct {
	Sid       string                              `json:"Sid,omitempty"`
	Effect    Effect                              `json:"Effect"`
	Action    StringOrSlice                       `json:"Action"`
	Resource  StringOrSlice                       `json:"Resource"`
	Principal *StringOrMap                        `json:"Principal,omitempty"`
	Condition map[string]map[string]StringOrSlice `json:"Condition,omitempty"`
}

// StringOrSlice supports both `"Action":"s3:GetObject"` and `"Action":["s3:GetObject","s3:HeadObject"]`.
type StringOrSlice []string

// StringOrMap supports `"Principal":"*"` and `"Principal":{"AWS":["..."]}`.
type StringOrMap struct {
	Star  bool
	Named map[string][]string
}
