package admin

func registerScrub(g router, d *Deps) {
	g.POST(routePathVolumeScrub, scrubVolumeHandler(d))
	g.POST(routePathScrub, wrapBody[ScrubReq, ScrubResp](d, TriggerScrub))
	g.GET(routePathScrubJobs, wrapZero(d, ListScrubJobs))
	g.GET(routePathScrubJob, scrubJobByIDHandler(d, GetScrubJob))
	g.DELETE(routePathScrubJob, scrubJobCancelHandler(d))
}

func registerScrubUI(g router, d *Deps) {
	g.POST(routePathVolumeScrub, scrubVolumeHandler(d))
	g.POST(routePathScrub, wrapBody[ScrubReq, ScrubResp](d, TriggerScrub))
	g.GET(routePathScrubJobs, wrapZero(d, ListScrubJobs))
	g.GET(routePathScrubJob, scrubJobByIDHandler(d, GetScrubJob))
}
