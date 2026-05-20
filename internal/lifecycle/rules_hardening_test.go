package lifecycle_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/lifecycle"
)

func enabled(id string) lifecycle.Rule {
	return lifecycle.Rule{ID: id, Status: lifecycle.StatusEnabled}
}

func with(r lifecycle.Rule, f func(*lifecycle.Rule)) lifecycle.Rule {
	f(&r)
	return r
}

func TestValidate_Hardening(t *testing.T) {
	utcMidnight := time.Date(2026, 5, 19, 0, 0, 0, 0, time.UTC)
	utcNoon := time.Date(2026, 5, 19, 12, 0, 0, 0, time.UTC)
	tru := true
	gt100 := int64(100)
	lt1000 := int64(1000)
	lt50 := int64(50)

	cases := []struct {
		name    string
		rule    lifecycle.Rule
		wantErr string
	}{
		{name: "expiration days ok",
			rule: with(enabled("r"), func(r *lifecycle.Rule) {
				r.Expiration = &lifecycle.Expiration{Days: 7}
			})},
		{name: "expiration date midnight ok",
			rule: with(enabled("r"), func(r *lifecycle.Rule) {
				r.Expiration = &lifecycle.Expiration{Date: &utcMidnight}
			})},
		{name: "expiration date noon rejected",
			rule: with(enabled("r"), func(r *lifecycle.Rule) {
				r.Expiration = &lifecycle.Expiration{Date: &utcNoon}
			}),
			wantErr: "midnight"},
		{name: "expiration days+date rejected",
			rule: with(enabled("r"), func(r *lifecycle.Rule) {
				r.Expiration = &lifecycle.Expiration{Days: 7, Date: &utcMidnight}
			}),
			wantErr: "Days and Date"},
		{name: "expired delete marker + days rejected",
			rule: with(enabled("r"), func(r *lifecycle.Rule) {
				r.Expiration = &lifecycle.Expiration{Days: 7, ExpiredObjectDeleteMarker: &tru}
			}),
			wantErr: "ExpiredObjectDeleteMarker"},
		{name: "expired delete marker alone ok",
			rule: with(enabled("r"), func(r *lifecycle.Rule) {
				r.Expiration = &lifecycle.Expiration{ExpiredObjectDeleteMarker: &tru}
			})},
		{name: "aws: tag rejected (top-level)",
			rule: with(enabled("r"), func(r *lifecycle.Rule) {
				r.Filter = &lifecycle.Filter{Tag: &lifecycle.Tag{Key: "aws:env", Value: "p"}}
				r.Expiration = &lifecycle.Expiration{Days: 1}
			}),
			wantErr: "aws:"},
		{name: "aws: tag rejected (in And)",
			rule: with(enabled("r"), func(r *lifecycle.Rule) {
				r.Filter = &lifecycle.Filter{And: &lifecycle.AndFilter{
					Prefix: "p/",
					Tags:   []lifecycle.Tag{{Key: "aws:internal", Value: "x"}},
				}}
				r.Expiration = &lifecycle.Expiration{Days: 1}
			}),
			wantErr: "aws:"},
		{name: "and with single criterion rejected",
			rule: with(enabled("r"), func(r *lifecycle.Rule) {
				r.Filter = &lifecycle.Filter{And: &lifecycle.AndFilter{Prefix: "p/"}}
				r.Expiration = &lifecycle.Expiration{Days: 1}
			}),
			wantErr: "And"},
		{name: "and with two criteria ok",
			rule: with(enabled("r"), func(r *lifecycle.Rule) {
				r.Filter = &lifecycle.Filter{And: &lifecycle.AndFilter{
					Prefix: "p/", ObjectSizeGreaterThan: &gt100,
				}}
				r.Expiration = &lifecycle.Expiration{Days: 1}
			})},
		{name: "top-level filter and And co-set rejected",
			rule: with(enabled("r"), func(r *lifecycle.Rule) {
				r.Filter = &lifecycle.Filter{
					Prefix: "p/",
					And:    &lifecycle.AndFilter{Prefix: "x/", ObjectSizeGreaterThan: &gt100},
				}
				r.Expiration = &lifecycle.Expiration{Days: 1}
			}),
			wantErr: "Filter"},
		{name: "size gt > size lt rejected",
			rule: with(enabled("r"), func(r *lifecycle.Rule) {
				r.Filter = &lifecycle.Filter{
					ObjectSizeGreaterThan: &lt1000,
					ObjectSizeLessThan:    &lt50,
				}
				r.Expiration = &lifecycle.Expiration{Days: 1}
			}),
			wantErr: "ObjectSize"},
		{name: "abort mpu + tag filter ok",
			rule: with(enabled("r"), func(r *lifecycle.Rule) {
				r.Filter = &lifecycle.Filter{Tag: &lifecycle.Tag{Key: "archive", Value: "yes"}}
				r.Expiration = &lifecycle.Expiration{Days: 7}
				r.AbortIncompleteMultipartUpload = &lifecycle.AbortIncompleteMultipartUpload{DaysAfterInitiation: 3}
			})},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &lifecycle.LifecycleConfiguration{Rules: []lifecycle.Rule{tc.rule}}
			err := lifecycle.Validate(cfg)
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}
