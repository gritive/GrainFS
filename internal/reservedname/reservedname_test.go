package reservedname

import "testing"

func TestIsInternalBucket(t *testing.T) {
	for _, c := range []struct {
		name string
		want bool
	}{
		{"_grainfs", true},
		{"_grainfs-audit", true},
		{"_grainfs-test", true},
		{"foo", false},
		{"default", false},
		{"_grain", false},
	} {
		if got := IsInternalBucket(c.name); got != c.want {
			t.Errorf("IsInternalBucket(%q) = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestIsReservedDefaultName(t *testing.T) {
	if !IsReservedDefaultName("default") {
		t.Fatal("default should be reserved")
	}
	if IsReservedDefaultName("default-2026") {
		t.Fatal("default-2026 is operator-creatable, not reserved")
	}
	if IsReservedDefaultName("mydefault") {
		t.Fatal("mydefault is operator-creatable, not reserved")
	}
}

func TestIsReservedBucketName(t *testing.T) {
	for _, c := range []struct {
		name string
		want bool
	}{
		{"_grainfs", true},
		{"_grainfs-audit", true},
		{"_grainfs-test", true},
		{"default", true},
		{"default-2026", false},
		{"mydefault", false},
		{"analytics", false},
		{"foo", false},
	} {
		if got := IsReservedBucketName(c.name); got != c.want {
			t.Errorf("IsReservedBucketName(%q) = %v, want %v", c.name, got, c.want)
		}
	}
}
