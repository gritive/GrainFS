package admin

func registerVolume(g router, d *Deps) {
	g.GET(routePathVolumes, wrapZero(d, ListVolumes))
	g.POST(routePathVolumes, wrapBody[CreateVolumeReq, VolumeInfo](d, CreateVolume))
	g.GET(routePathVolume, wrapName(d, GetVolume))
	g.DELETE(routePathVolume, deleteVolumeHandler(d))
	g.GET(routePathVolumeStat, wrapName(d, StatVolume))
	g.POST(routePathVolumeResize, wrapNameBody[ResizeReq, ResizeResp](d, ResizeVolume))
	g.POST(routePathVolumeRecalculate, wrapName(d, RecalculateVolume))
	g.POST(routePathVolumeClone, wrapBodyNoOut[CloneReq](d, CloneVolume))
	g.POST(routePathVolumeWriteAt, wrapBody[WriteAtVolumeReq, WriteAtVolumeResp](d, WriteAtVolume))
	g.POST(routePathVolumeReadAt, wrapBody[ReadAtVolumeReq, ReadAtVolumeResp](d, ReadAtVolume))
}

func registerVolumeUI(g router, d *Deps) {
	g.GET(routePathVolumes, wrapZero(d, ListVolumes))
	g.POST(routePathVolumes, wrapBody[CreateVolumeReq, VolumeInfo](d, CreateVolume))
	g.GET(routePathVolume, wrapName(d, GetVolume))
	g.GET(routePathVolumeStat, wrapName(d, StatVolume))
}

func registerSnapshot(g router, d *Deps) {
	g.POST(routePathVolumeSnapshots, wrapName(d, CreateSnapshot))
	g.GET(routePathVolumeSnapshots, wrapName(d, ListSnapshots))
	g.DELETE(routePathVolumeSnapshot, deleteSnapshotHandler(d))
	g.POST(routePathVolumeSnapshotRollback, rollbackHandler(d))
}

func registerSnapshotUI(g router, d *Deps) {
	g.POST(routePathVolumeSnapshots, wrapName(d, CreateSnapshot))
	g.GET(routePathVolumeSnapshots, wrapName(d, ListSnapshots))
}
