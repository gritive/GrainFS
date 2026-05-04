package admin

import (
	"context"
	"fmt"
)

// DashboardTokenResp describes the dashboard URL + token to display to the operator.
type DashboardTokenResp struct {
	URL          string `json:"url"`
	Host         string `json:"host"`
	Token        string `json:"token"`
	Path         string `json:"path"`
	PublicURLSet bool   `json:"public_url_set"`
}

func GetDashboardToken(ctx context.Context, d *Deps) (DashboardTokenResp, error) {
	if d.Token == nil {
		return DashboardTokenResp{}, NewInternal("dashboard token store not configured")
	}
	return buildDashboardResp(d, d.Token.Get()), nil
}

func RotateDashboardToken(ctx context.Context, d *Deps) (DashboardTokenResp, error) {
	if d.Token == nil {
		return DashboardTokenResp{}, NewInternal("dashboard token store not configured")
	}
	tok, err := d.Token.Rotate()
	if err != nil {
		return DashboardTokenResp{}, NewInternal(err.Error())
	}
	return buildDashboardResp(d, tok), nil
}

func buildDashboardResp(d *Deps, tok string) DashboardTokenResp {
	base := d.PublicURL
	publicSet := base != ""
	if base == "" {
		base = "http://localhost:9000"
	}
	path := ""
	if d.Token != nil {
		path = d.Token.Path()
	}
	return DashboardTokenResp{
		URL:          fmt.Sprintf("%s/ui/#token=%s", base, tok),
		Host:         base,
		Token:        tok,
		Path:         path,
		PublicURLSet: publicSet,
	}
}
