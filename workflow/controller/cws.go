package controller

import (
	"encoding/json"
	"github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"io"
	"net/http"
	"strconv"
	"strings"
)

type registerWorkflowRequestBody struct {
	//VolumeClaims []volumeClaims `json:"volumeClaims"`
	//WorkDir string `json:"workDir"`
	Dns          string `json:"dns"`
	TraceEnabled bool   `json:"traceEnabled"`
	Namespace    string `json:"namespace"`
	//CostFunction string `json:"costFunction"`
	Strategy string `json:"strategy"`
	//MaxCopyTaskPerNode int `json:"maxCopyTaskPerNode"`
	//MaxWaitingCopyTaskPerNode int `json:"maxWaitingCopyTaskPerNode"`
}

type vertex struct {
	Label string `json:"label"`
	Uid   int    `json:"uid"`
	Rank  int    `json:"rank"`
	Type  string `json:"type"`
}

type taskParams struct{}
type taskInputs struct{}

type task struct {
	Task            string     `json:"task"`
	Name            string     `json:"name"`
	SchedulerParams taskParams `json:"schedulerParams"`
	Inputs          taskInputs `json:"inputs"`
	RunName         string     `json:"runName"`
	Cpus            int        `json:"cpus"`
	MemoryInBytes   int        `json:"memoryInBytes"`
	WorkDir         string     `json:"workDir"`
}

// TODO: localhost:8081 only works if the controller runs
// outside the cluster and we enable port forwarding for the cws
// if the controller runs as a pod in the cluster we would use
// "http://workflow-scheduler" as the url. Ideally there is one
// solution for both cases

// TODO: close response bodies if necessary

var cwsURL = "http://localhost:8081"
var registeredTasks = 0

func (woc *wfOperationCtx) cws() {
	//deleteWF()
	if !woc.execWf.Status.RegisteredWithCWS {
		if woc.registerWF() && woc.submitDAG() {
			woc.wf.Status.RegisteredWithCWS = true
		} else {
			woc.cwslog("error")
		}
	}
}

func (woc *wfOperationCtx) cwslog(msg string) {
	woc.log.Info("cws: " + msg)
}

func (woc *wfOperationCtx) registerWF() bool {
	woc.cwslog("registering workflow")
	body := registerWorkflowRequestBody{
		Dns:          "",
		TraceEnabled: true,
		Namespace:    "argo",
		Strategy:     "fifo-fair",
	}
	jsonBytes, err := json.Marshal(body)
	if err != nil {
		woc.cwslog(err.Error())
		return false
	}
	jsonString := string(jsonBytes)
	woc.cwslog("..." + jsonString)
	resp, err := http.Post(cwsURL+"/v1/scheduler/"+woc.execWf.ObjectMeta.Name, "application/json", strings.NewReader(jsonString))
	if err != nil {
		woc.cwslog(err.Error())
		return false
	}
	// TODO: read response code and handle errors
	if resp.StatusCode != 200 {
		woc.cwslog("..." + resp.Status)
		return false
	}
	woc.cwslog("...ok")
	return true
}

func (woc *wfOperationCtx) deleteWF() bool {
	woc.cwslog("deleting workflow")
	client := &http.Client{}
	req, err := http.NewRequest("DELETE", cwsURL+"/v1/scheduler/"+woc.execWf.ObjectMeta.Name, nil)
	if err != nil {
		woc.cwslog(err.Error())
		return false
	}
	resp, err := client.Do(req)
	if err != nil {
		woc.cwslog(err.Error())
		return false
	}
	if resp.StatusCode != 200 {
		woc.cwslog("..." + resp.Status)
		return false
	}
	woc.cwslog("...ok")
	return true
}

func (woc *wfOperationCtx) submitDAG() bool {
	woc.cwslog("submitting DAG")
	// TODO: understand vertex fields
	body := []vertex{{
		Label: "testpod",
		Uid:   0,
		Rank:  0,
		Type:  "PROCESS",
	}}
	jsonBytes, err := json.Marshal(body)
	if err != nil {
		woc.cwslog(err.Error())
		return false
	}
	jsonString := string(jsonBytes)
	woc.cwslog(jsonString)
	resp, err := http.Post(cwsURL+"/v1/scheduler/"+woc.execWf.ObjectMeta.Name+"/DAG/vertices", "application/json", strings.NewReader(jsonString))
	if err != nil {
		woc.cwslog(err.Error())
		return false
	}
	// TODO: read response code and handle errors
	response_body, err := io.ReadAll(resp.Body)
	woc.cwslog(string(response_body))
	if resp.StatusCode != 200 {
		woc.cwslog("..." + resp.Status)
		return false
	}
	woc.cwslog("...ok")
	return true

}

func (woc *wfOperationCtx) startBatch() bool {
	woc.cwslog("start batch")
	client := &http.Client{}
	req, err := http.NewRequest("PUT", cwsURL+"/v1/scheduler/"+woc.execWf.ObjectMeta.Name+"/startBatch", nil)
	resp, err := client.Do(req)
	if err != nil {
		woc.cwslog(err.Error())
		return false
	}
	// TODO: read response code and handle errors
	if resp.StatusCode != 200 {
		woc.cwslog("..." + resp.Status)
		return false
	}
	woc.cwslog("...ok")
	return true
}

func (woc *wfOperationCtx) endBatch() bool {
	woc.cwslog("end batch")
	client := &http.Client{}
	req, err := http.NewRequest("PUT", cwsURL+"/v1/scheduler/"+woc.execWf.ObjectMeta.Name+"/endBatch", strings.NewReader(strconv.Itoa(registeredTasks)))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		woc.cwslog(err.Error())
		return false
	}
	// TODO: read response code and handle errors
	if resp.StatusCode != 200 {
		woc.cwslog("..." + resp.Status)
		return false
	}
	registeredTasks = 0
	woc.cwslog("...ok")
	return true
}

func (woc *wfOperationCtx) registerTask(node *v1alpha1.NodeStatus) bool {
	woc.cwslog("registering task")
	// TODO: understand task fields
	body := task{
		Task:            "testpod",
		Name:            "testpod",
		SchedulerParams: taskParams{},
		Inputs:          taskInputs{},
		RunName:         node.Name,
		Cpus:            1,
		MemoryInBytes:   128000000,
		WorkDir:         "/",
	}
	jsonBytes, err := json.Marshal(body)
	if err != nil {
		woc.cwslog(err.Error())
		return false
	}
	jsonString := string(jsonBytes)
	woc.cwslog(jsonString)
	resp, err := http.Post(cwsURL+"/v1/scheduler/"+woc.execWf.ObjectMeta.Name+"/task/0", "application/json", strings.NewReader(jsonString))
	if err != nil {
		woc.cwslog(err.Error())
		return false
	}
	// TODO: read response code and handle errors
	if resp.StatusCode != 200 {
		woc.cwslog("..." + resp.Status)
		return false
	}
	registeredTasks++
	woc.cwslog("...ok")
	return true
}
