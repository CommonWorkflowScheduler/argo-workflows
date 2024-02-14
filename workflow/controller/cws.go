package controller

import (
	"net/http"
)

func cws(woc *wfOperationCtx) {
	cwslog := startlog(woc)
	cwslog("trying to connect to workflow-scheduler")
	// TODO: localhost:8081 only works if the controller runs
	// outside the cluster and we enable port forwarding for the cws
	// if the controller runs as a pod in the cluster we would use
	// "http://workflow-scheduler" as the url. Ideally there is one
	// solution for both cases
	resp, err := http.Get("http://localhost:8081/swagger-ui.html")
	if err != nil {
		cwslog(err.Error())
		return
	}
	cwslog(resp.Status)
}

func startlog(woc *wfOperationCtx) func(string) {
	return func (msg string) {
		woc.log.Info("cws: ", msg)
	}
}
