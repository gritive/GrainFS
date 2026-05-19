package policy

import "testing"

func TestParse_AcceptsMinimal(t *testing.T) {
	doc := []byte(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::a/*"}]}`)
	p, err := Parse(doc)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(p.Statement) != 1 {
		t.Fatalf("statements = %d", len(p.Statement))
	}
	if p.Statement[0].Effect != EffectAllow {
		t.Fatalf("effect = %v", p.Statement[0].Effect)
	}
}

func TestParse_AcceptsAbsentVersion(t *testing.T) {
	doc := []byte(`{"Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`)
	if _, err := Parse(doc); err != nil {
		t.Fatalf("Parse: %v", err)
	}
}

func TestParse_RejectsNotAction(t *testing.T) {
	doc := []byte(`{"Statement":[{"Effect":"Allow","NotAction":"s3:GetObject","Resource":"*"}]}`)
	_, err := Parse(doc)
	if err == nil || err.Error() == "" {
		t.Fatal("expected rejection for NotAction")
	}
}

func TestParse_RejectsUnsupportedConditionKey(t *testing.T) {
	doc := []byte(`{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*","Condition":{"StringEquals":{"aws:UserAgent":"x"}}}]}`)
	_, err := Parse(doc)
	if err == nil {
		t.Fatal("expected rejection for aws:UserAgent")
	}
}

func TestParse_RejectsMalformedARN(t *testing.T) {
	doc := []byte(`{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"notanarn"}]}`)
	if _, err := Parse(doc); err == nil {
		t.Fatal("expected ARN rejection")
	}
}

func TestParse_AcceptsBothCondKeys(t *testing.T) {
	doc := []byte(`{"Statement":[{"Effect":"Allow","Action":"s3:ListBucket","Resource":"arn:aws:s3:::a","Condition":{"IpAddress":{"aws:SourceIp":"10.0.0.0/8"},"StringLike":{"s3:prefix":"logs/*"}}}]}`)
	if _, err := Parse(doc); err != nil {
		t.Fatalf("Parse: %v", err)
	}
}
