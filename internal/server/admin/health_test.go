package admin

import (
	"reflect"
	"testing"

	"github.com/gritive/GrainFS/internal/incident"
	"github.com/gritive/GrainFS/internal/volume"
)

func TestAnnotateVolumeHealth_TableDriven(t *testing.T) {
	bucket := volume.VolumeBucketName
	blockPrefix := volume.BlockKeyPrefix("v1")

	mkInc := func(b, k string, sev incident.Severity, state incident.State) incident.IncidentState {
		return incident.IncidentState{
			ID:       "i-" + k,
			Scope:    incident.Scope{Bucket: b, Key: k},
			Severity: sev,
			State:    state,
		}
	}

	cases := []struct {
		name        string
		incidents   []incident.IncidentState
		wantHealth  string
		wantReasons []string
	}{
		{
			name:        "no incidents",
			incidents:   nil,
			wantHealth:  "ok",
			wantReasons: []string{},
		},
		{
			name:        "incident in other bucket ignored",
			incidents:   []incident.IncidentState{mkInc("other", "v1", incident.SeverityWarning, incident.StateActing)},
			wantHealth:  "ok",
			wantReasons: []string{},
		},
		{
			name:        "incident on volume name with warning",
			incidents:   []incident.IncidentState{mkInc(bucket, "v1", incident.SeverityWarning, incident.StateActing)},
			wantHealth:  "warning",
			wantReasons: []string{"recent_incident"},
		},
		{
			name:        "incident on block prefix with warning",
			incidents:   []incident.IncidentState{mkInc(bucket, blockPrefix+"00000001", incident.SeverityWarning, incident.StateActing)},
			wantHealth:  "warning",
			wantReasons: []string{"recent_incident"},
		},
		{
			name:        "critical severity",
			incidents:   []incident.IncidentState{mkInc(bucket, "v1", incident.SeverityCritical, incident.StateActing)},
			wantHealth:  "critical",
			wantReasons: []string{"recent_incident"},
		},
		{
			name:        "blocked state -> critical",
			incidents:   []incident.IncidentState{mkInc(bucket, "v1", incident.SeverityWarning, incident.StateBlocked)},
			wantHealth:  "critical",
			wantReasons: []string{"recent_incident"},
		},
		{
			name:        "needs-human state -> critical",
			incidents:   []incident.IncidentState{mkInc(bucket, "v1", incident.SeverityWarning, incident.StateNeedsHuman)},
			wantHealth:  "critical",
			wantReasons: []string{"recent_incident"},
		},
		{
			name:        "proof-unavailable state -> critical",
			incidents:   []incident.IncidentState{mkInc(bucket, "v1", incident.SeverityWarning, incident.StateProofUnavailable)},
			wantHealth:  "critical",
			wantReasons: []string{"recent_incident"},
		},
		{
			name:        "fixed state ignored",
			incidents:   []incident.IncidentState{mkInc(bucket, "v1", incident.SeverityCritical, incident.StateFixed)},
			wantHealth:  "ok",
			wantReasons: []string{},
		},
		{
			name:        "isolated state ignored",
			incidents:   []incident.IncidentState{mkInc(bucket, "v1", incident.SeverityCritical, incident.StateIsolated)},
			wantHealth:  "ok",
			wantReasons: []string{},
		},
		{
			name: "warning then critical merges to critical",
			incidents: []incident.IncidentState{
				mkInc(bucket, "v1", incident.SeverityWarning, incident.StateActing),
				mkInc(bucket, "v1", incident.SeverityCritical, incident.StateActing),
			},
			wantHealth:  "critical",
			wantReasons: []string{"recent_incident"},
		},
		{
			name: "duplicate reason deduped",
			incidents: []incident.IncidentState{
				mkInc(bucket, "v1", incident.SeverityWarning, incident.StateActing),
				mkInc(bucket, "v1", incident.SeverityWarning, incident.StateActing),
			},
			wantHealth:  "warning",
			wantReasons: []string{"recent_incident"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			vols := []VolumeInfo{{Name: "v1", Health: "ok", HealthReasons: []string{}}}
			annotateVolumeHealth(vols, tc.incidents, nil)
			if vols[0].Health != tc.wantHealth {
				t.Errorf("health = %q, want %q", vols[0].Health, tc.wantHealth)
			}
			if !reflect.DeepEqual(vols[0].HealthReasons, tc.wantReasons) {
				t.Errorf("reasons = %v, want %v", vols[0].HealthReasons, tc.wantReasons)
			}
		})
	}
}

func TestAnnotateVolumeHealth_MultipleVolumesIndependent(t *testing.T) {
	bucket := volume.VolumeBucketName
	vols := []VolumeInfo{
		{Name: "v1", Health: "ok", HealthReasons: []string{}},
		{Name: "v2", Health: "ok", HealthReasons: []string{}},
	}
	incidents := []incident.IncidentState{
		{Scope: incident.Scope{Bucket: bucket, Key: "v1"}, Severity: incident.SeverityWarning, State: incident.StateActing},
	}
	annotateVolumeHealth(vols, incidents, nil)
	if vols[0].Health != "warning" {
		t.Errorf("v1 health = %q, want warning", vols[0].Health)
	}
	if vols[1].Health != "ok" {
		t.Errorf("v2 health = %q, want ok", vols[1].Health)
	}
}

func TestIncidentMatchesVolume(t *testing.T) {
	bucket := volume.VolumeBucketName
	blockPrefix := volume.BlockKeyPrefix("v1")
	cases := []struct {
		name string
		st   incident.IncidentState
		want bool
	}{
		{"name match", incident.IncidentState{Scope: incident.Scope{Bucket: bucket, Key: "v1"}}, true},
		{"block prefix match", incident.IncidentState{Scope: incident.Scope{Bucket: bucket, Key: blockPrefix + "abc"}}, true},
		{"different bucket", incident.IncidentState{Scope: incident.Scope{Bucket: "other", Key: "v1"}}, false},
		{"unrelated key", incident.IncidentState{Scope: incident.Scope{Bucket: bucket, Key: "v2"}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := incidentMatchesVolume(tc.st, "v1", blockPrefix)
			if got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}
