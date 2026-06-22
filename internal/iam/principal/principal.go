package principal

type Kind string

const (
	KindServiceAccount     Kind = "sa"
	KindOIDC               Kind = "oidc"
	KindProtocolCredential Kind = "protocol_credential"
)

type Principal struct {
	Kind         Kind
	ID           string
	Issuer       string
	Subject      string
	Groups       []string
	Source       string
	CredentialID string
}

func ServiceAccount(id string) Principal {
	return Principal{Kind: KindServiceAccount, ID: id, Source: "iam"}
}

func ProtocolCredential(ownerSAID, credentialID string) Principal {
	return Principal{Kind: KindProtocolCredential, ID: ownerSAID, Source: "protocol-credential", CredentialID: credentialID}
}

func OIDC(issuer, subject, id string, groups []string) Principal {
	return Principal{
		Kind:    KindOIDC,
		ID:      id,
		Issuer:  issuer,
		Subject: subject,
		Groups:  append([]string(nil), groups...),
		Source:  "oidc",
	}
}

func (p Principal) GroupNames() []string {
	return append([]string(nil), p.Groups...)
}
