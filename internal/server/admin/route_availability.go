package admin

type routeFeature uint8

const (
	routeFeatureBuckets routeFeature = iota
	routeFeatureNfsExports
	routeFeatureIAM
)

type routeUnavailableMode uint8

const (
	routeHiddenWhenUnavailable routeUnavailableMode = iota
)

type routeAvailabilityEntry struct {
	feature         routeFeature
	name            string
	unavailableMode routeUnavailableMode
}

var routeAvailabilityManifest = []routeAvailabilityEntry{
	{feature: routeFeatureBuckets, name: "buckets", unavailableMode: routeHiddenWhenUnavailable},
	{feature: routeFeatureNfsExports, name: "nfs_exports", unavailableMode: routeHiddenWhenUnavailable},
	{feature: routeFeatureIAM, name: "iam", unavailableMode: routeHiddenWhenUnavailable},
}

func routeAvailabilityForFeature(feature routeFeature) routeAvailabilityEntry {
	for _, entry := range routeAvailabilityManifest {
		if entry.feature == feature {
			return entry
		}
	}
	return routeAvailabilityEntry{feature: feature, unavailableMode: routeHiddenWhenUnavailable}
}

func routeFeatureAvailable(d *Deps, feature routeFeature) bool {
	if d == nil {
		return false
	}
	switch feature {
	case routeFeatureBuckets:
		return d.Buckets != nil
	case routeFeatureNfsExports:
		return d.NfsExports != nil
	case routeFeatureIAM:
		return d.IAM != nil
	default:
		return false
	}
}

func routeFeatureRoutesVisible(d *Deps, feature routeFeature) bool {
	if routeFeatureAvailable(d, feature) {
		return true
	}
	return routeAvailabilityForFeature(feature).unavailableMode != routeHiddenWhenUnavailable
}
