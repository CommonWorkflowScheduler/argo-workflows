package controller

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
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
	Type  string `json:"type"`
}

type edge struct {
	Label string `json:"label"`
	Uid   int    `json:"uid"`
	From  int    `json:"from"`
	To    int    `json:"to"`
}

type taskParams struct{}

type fileHolder struct {
	StorePath string `json:"storePath"`
	SourceObj string `json:"sourceObj"`
	StageName string `json:"stageName"`
}

type taskInputs struct {
	BooleanInputs []struct{}   `json:"booleanInputs"` // NOTE: never used by CWS scheduler
	NumberInputs  []struct{}   `json:"numberInputs"`  // NOTE: never used by CWS scheduler
	StringInputs  []struct{}   `json:"stringInputs"`  // NOTE: never used by CWS scheduler
	FileInputs    []fileHolder `json:"fileInputs"`
}

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

var registeredTasks = 0
var tasksInBatch = 0

func (woc *wfOperationCtx) cwsInit() bool {
	_, ok := woc.globalParams["cwsSchedulerUrl"]
	if !ok {
		schedulerName, ok := woc.globalParams["cwsSchedulerName"]
		if !ok {
			schedulerName = "workflow-scheduler"
			woc.globalParams["cwsSchedulerName"] = schedulerName
		}
		schedulerPodName, ok := woc.globalParams["cwsSchedulerPodName"]
		if !ok {
			schedulerPodName = "workflow-scheduler"
			woc.globalParams["cwsSchedulerPodName"] = schedulerPodName
		}
		pods := woc.controller.kubeclientset.CoreV1().Pods(woc.wf.Namespace)
		pod, err := pods.Get(context.Background(), schedulerPodName, metav1.GetOptions{})
		if err != nil {
			woc.log.Error("cws: could not register workflow and submit dag")
		}
		schedulerPort, ok := woc.globalParams["cwsSchedulerPort"]
		if !ok {
			schedulerPort = "8080"
		}
		podIp := pod.Status.PodIP
		schedulerUrl := "http://" + podIp + ":" + schedulerPort
		woc.log.Info("cws: scheduler at " + schedulerUrl)
		woc.globalParams["cwsSchedulerUrl"] = schedulerUrl
	}

	if !woc.execWf.Status.RegisteredWithCWS {
		if !woc.cwsRegisterWF() {
			woc.log.Error("cws: Unable to register Workflow to CWS Scheduler")
			return false
		}
		if !woc.cwsSubmitDAG() {
			woc.log.Error("cws: Unable to submit Workflow DAG to CWS Scheduler")
			return false
		}
		woc.wf.Status.RegisteredWithCWS = true
	}
	return true
}

func (woc *wfOperationCtx) cwsExecutionName() string {
	// woc.execWf.ObjectMeta.Name
	return "friedrich"
}

func (woc *wfOperationCtx) cwsRegisterWF() bool {
	woc.log.Info("cws: registering workflow")
	body := registerWorkflowRequestBody{
		Dns:          "",
		TraceEnabled: true,
		Namespace:    "argo",
		Strategy:     "fifo-fair", // TODO: parameterize
	}
	jsonBytes, err := json.Marshal(body)
	if err != nil {
		woc.log.Error("cws: " + err.Error())
		return false
	}
	jsonString := string(jsonBytes)
	woc.log.Info("cws: " + jsonString)
	url := woc.globalParams["cwsSchedulerUrl"] + "/v1/scheduler/" + woc.cwsExecutionName()
	resp, err := http.Post(url, "application/json", strings.NewReader(jsonString))
	if err != nil {
		woc.log.Error("cws: " + err.Error())
		return false
	}
	if resp.StatusCode != 200 {
		woc.log.Error("cws: " + resp.Status)
		return false
	}
	woc.log.Info("cws: successfully registered workflow")
	return true
}

func (woc *wfOperationCtx) cwsDeleteWF() bool {
	woc.log.Info("cws: deleting workflow")
	client := &http.Client{}
	url := woc.globalParams["cwsSchedulerUrl"] + "/v1/scheduler/" + woc.cwsExecutionName()
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		woc.log.Error("cws: " + err.Error())
		return false
	}
	resp, err := client.Do(req)
	if err != nil {
		woc.log.Error("cws: " + err.Error())
		return false
	}
	if resp.StatusCode != 200 {
		woc.log.Error("cws: " + resp.Status)
		return false
	}
	woc.log.Info("cws: successfully deleted workflow")
	return true
}

// submit the DAG of the workflow to the CWS scheduler
// NOTE: we (for now) only support workflows that have either:
//   - only one template definition (which is also the entry point)
//     i.e. no template invocations
//   - a single DAG template as the entry point that only calls template definitions
//     i.e. it calls no template invocations including itself
func (woc *wfOperationCtx) cwsSubmitDAG() bool {
	woc.log.Info("cws: submitting DAG")
	entrypointUid := 0
	vertices := []vertex{{
		Label: "Entrypoint",
		Uid:   entrypointUid,
		Type:  "ORIGIN",
	}}
	edges := []edge{}

	var entrypointDag *v1alpha1.DAGTemplate

	for _, template := range woc.wf.Spec.Templates {
		template.SchedulerName = woc.globalParams["cwsSchedulerName"] + "-" + woc.cwsExecutionName()
		templType := template.GetType()
		switch templType {
		case v1alpha1.TemplateTypeDAG:
			if template.Name != woc.wf.Spec.Entrypoint {
				woc.log.Error("cws: DAG templates that are not the entrypoint are not supported with CWS")
				return false
			}
			entrypointDag = template.DAG
		case v1alpha1.TemplateTypeSteps:
			woc.log.Error("cws: Steps templates are not supported with CWS")
			return false
		default:
			vertexUid := len(vertices)
			template_vertex := vertex{
				Label: template.Name,
				Uid:   vertexUid,
				Type:  "PROCESS",
			}
			vertices = append(vertices, template_vertex)
			if template.Name == woc.wf.Spec.Entrypoint {
				uid := len(edges)
				edges = append(edges, edge{
					Label: strconv.Itoa(uid),
					Uid:   uid,
					From:  entrypointUid,
					To:    vertexUid,
				})
			}
		}
	}

	if entrypointDag != nil {
		if len(edges) > 0 {
			woc.log.Error("cws: Did not expect second entrypoint when using DAG template as an entrypoint")
			return false
		}
		vertexUids := make(map[string]int)
		for _, vertex := range vertices {
			if vertex.Label != "PROCESS" {
				continue
			}
			vertexUids[vertex.Label] = vertex.Uid
		}

		for _, task := range entrypointDag.Tasks {
			if task.Inline != nil {
				woc.log.Error("cws: Inline templates are not supported with CWS")
				return false
			}
			templateUid := vertexUids[task.Template]
			if templateUid == 0 {
				woc.log.Error("cws: Expect DAG tasks to have valid template name")
				return false
			}
			for _, dependency := range task.Dependencies {
				dependencyUid := vertexUids[dependency]
				if dependencyUid == 0 {
					woc.log.Error("cws: Expect DAG tasks to have non-invoker dependencies with valid names")
					return false
				}
				edges = append(edges, edge{
					Label: "",
					Uid:   len(edges),
					From:  dependencyUid,
					To:    templateUid,
				})
			}
		}

	}

	jsonBytes, err := json.Marshal(vertices)
	if err != nil {
		woc.log.Error("cws: " + err.Error())
		return false
	}
	jsonString := string(jsonBytes)
	woc.log.Info("cws: " + jsonString)
	url := woc.globalParams["cwsSchedulerUrl"] + "/v1/scheduler/" + woc.cwsExecutionName() + "/DAG/vertices"
	resp, err := http.Post(url, "application/json", strings.NewReader(jsonString))
	if err != nil {
		woc.log.Error("cws: " + err.Error())
		return false
	}
	response_body, err := io.ReadAll(resp.Body)
	woc.log.Info("cws: " + string(response_body))
	if resp.StatusCode != 200 {
		woc.log.Error("cws: " + resp.Status)
		return false
	}

	jsonBytes, err = json.Marshal(edges)
	if err != nil {
		woc.log.Error("cws: " + err.Error())
		return false
	}
	jsonString = string(jsonBytes)
	woc.log.Info("cws: " + jsonString)
	url = woc.globalParams["cwsSchedulerUrl"] + "/v1/scheduler/" + woc.cwsExecutionName() + "/DAG/edges"
	resp, err = http.Post(url, "application/json", strings.NewReader(jsonString))
	if err != nil {
		woc.log.Error("cws: " + err.Error())
		return false
	}
	response_body, err = io.ReadAll(resp.Body)
	woc.log.Info("cws: " + string(response_body))
	if resp.StatusCode != 200 {
		woc.log.Error("cws: " + resp.Status)
		return false
	}

	woc.log.Info("cws: successfully submitted DAG")
	return true

}

func (woc *wfOperationCtx) cwsStartBatch() bool {
	woc.log.Info("cws: starting batch")
	client := &http.Client{}
	url := woc.globalParams["cwsSchedulerUrl"] + "/v1/scheduler/" + woc.cwsExecutionName() + "/startBatch"
	req, err := http.NewRequest("PUT", url, nil)
	resp, err := client.Do(req)
	if err != nil {
		woc.log.Error("cws: " + err.Error())
		return false
	}
	if resp.StatusCode != 200 {
		woc.log.Error("cws: " + resp.Status)
		return false
	}
	woc.log.Info("cws: successfully started batch")
	tasksInBatch = 0
	return true
}

func (woc *wfOperationCtx) cwsEndBatch() bool {
	woc.log.Info("cws: ending batch")
	client := &http.Client{}
	url := woc.globalParams["cwsSchedulerUrl"] + "/v1/scheduler/" + woc.cwsExecutionName() + "/endBatch"
	req, err := http.NewRequest("PUT", url, strings.NewReader(strconv.Itoa(tasksInBatch)))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		woc.log.Error("cws: " + err.Error())
		return false
	}
	if resp.StatusCode != 200 {
		woc.log.Error("cws: " + resp.Status)
		return false
	}
	woc.log.Info("cws: successfully ended batch")
	return true
}

func (woc *wfOperationCtx) cwsAddMetadata(node *v1alpha1.NodeStatus, meta *v1alpha1.Metadata) {
	meta.Labels["app"] = "argo"
	meta.Labels["processName"] = node.TemplateName
	meta.Labels["runName"] = node.Name
	meta.Labels["taskName"] = node.DisplayName
}

func (woc *wfOperationCtx) cwsRegisterTask(node *v1alpha1.NodeStatus) bool {
	woc.log.Info("cws: registering task")
	// TODO: understand task fields
	body := task{
		Task:            node.TemplateName,
		Name:            node.DisplayName,
		SchedulerParams: taskParams{},
		Inputs:          taskInputs{}, // NOTE: only file inputs matter
		RunName:         node.Name,
		Cpus:            0, // NOTE: never used by CWS scheduler
		MemoryInBytes:   0, // NOTE: never used by CWS scheduler
		WorkDir:         "/",
	}
	jsonBytes, err := json.Marshal(body)
	if err != nil {
		woc.log.Error("cws: " + err.Error())
		return false
	}
	jsonString := string(jsonBytes)
	woc.log.Info("cws: " + jsonString)
	url := woc.globalParams["cwsSchedulerUrl"] + "/v1/scheduler/" + woc.cwsExecutionName() + "/task/"
	resp, err := http.Post(url+strconv.Itoa(registeredTasks), "application/json", strings.NewReader(jsonString))
	if err != nil {
		woc.log.Error("cws: " + err.Error())
		return false
	}
	if resp.StatusCode != 200 {
		woc.log.Error("cws: " + resp.Status)
		return false
	}
	registeredTasks++
	woc.log.Info("cws: successfully registered task")
	return true
}
