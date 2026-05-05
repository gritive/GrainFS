package server

import "github.com/gritive/GrainFS/internal/policy"

type BucketPolicy = policy.BucketPolicy
type PolicyStatement = policy.PolicyStatement
type PolicyPrincipal = policy.PolicyPrincipal

var ParsePolicy = policy.ParsePolicy
