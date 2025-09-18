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
	"github.com/argoproj/argo-workflows/v3/util/logging"
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

const (
	strategyKey = "cwsSchedulerStrategy"
	nameKey     = "cwsSchedulerName"
	podNameKey  = "cwsSchedulerPodName"
	podPortKey  = "cwsSchedulerPort"
	urlKey      = "cwsSchedulerUrl"
)

var registeredTasks = 0
var tasksInBatch = 0

func (woc *wfOperationCtx) cwsInit(ctx context.Context) bool {
	schedulerStrategy, ok := woc.globalParams[strategyKey]
	if !ok {
		schedulerStrategy = "fifo-fair"
		woc.globalParams[strategyKey] = schedulerStrategy
	}
	schedulerName, ok := woc.globalParams[nameKey]
	if !ok {
		schedulerName = "workflow-scheduler"
		woc.globalParams[nameKey] = schedulerName
	}

	_, ok = woc.globalParams[urlKey]
	if !ok {
		schedulerPodName, ok := woc.globalParams[podNameKey]
		if !ok {
			schedulerPodName = "workflow-scheduler"
		}
		pods := woc.controller.kubeclientset.CoreV1().Pods(woc.wf.Namespace)
		pod, err := pods.Get(context.Background(), schedulerPodName, metav1.GetOptions{})
		if err != nil {
			woc.log.Error(ctx, "cws: could not register workflow and submit dag")
			return false
		}
		schedulerPort, ok := woc.globalParams[podPortKey]
		if !ok {
			schedulerPort = "8080"
		}
		podIp := pod.Status.PodIP
		schedulerUrl := "http://" + podIp + ":" + schedulerPort
		woc.log.Info(ctx, "cws: scheduler at "+schedulerUrl)
		woc.globalParams[urlKey] = schedulerUrl
	}

	if !woc.execWf.Status.RegisteredWithCWS {
		if !woc.cwsRegisterWF(ctx) {
			woc.log.Error(ctx, "cws: Unable to register Workflow to CWS Scheduler")
			return false
		}
		if !woc.cwsSubmitDAG(ctx) {
			woc.log.Error(ctx, "cws: Unable to submit Workflow DAG to CWS Scheduler")
			return false
		}
		woc.wf.Status.RegisteredWithCWS = true
	}
	return true
}

func (woc *wfOperationCtx) cwsExecutionName() string {
	return "argo-" + string(woc.execWf.UID)
}

func (woc *wfOperationCtx) cwsRegisterWF(ctx context.Context) bool {
	woc.log.Info(ctx, "cws: registering workflow (execution: "+woc.cwsExecutionName()+")")
	body := registerWorkflowRequestBody{
		Dns:          "",
		TraceEnabled: true,
		Namespace:    "argo",
		Strategy:     woc.globalParams[strategyKey],
	}
	jsonBytes, err := json.Marshal(body)
	if err != nil {
		woc.log.Error(ctx, "cws: "+err.Error())
		return false
	}
	jsonString := string(jsonBytes)
	woc.log.Info(ctx, "cws: "+jsonString)
	url := woc.globalParams[urlKey] + "/v1/scheduler/" + woc.cwsExecutionName()
	resp, err := http.Post(url, "application/json", strings.NewReader(jsonString))
	if err != nil {
		woc.log.Error(ctx, "cws: "+err.Error())
		return false
	}
	if resp.StatusCode != 200 {
		woc.log.Error(ctx, "cws: "+resp.Status)
		return false
	}
	woc.log.Info(ctx, "cws: successfully registered workflow")
	return true
}

func (woc *wfOperationCtx) cwsDeleteWF(ctx context.Context) bool {
	woc.log.Info(ctx, "cws: deleting workflow")
	client := &http.Client{}
	url := woc.globalParams[urlKey] + "/v1/scheduler/" + woc.cwsExecutionName()
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		woc.log.Error(ctx, "cws: "+err.Error())
		return false
	}
	resp, err := client.Do(req)
	if err != nil {
		woc.log.Error(ctx, "cws: "+err.Error())
		return false
	}
	if resp.StatusCode != 200 {
		woc.log.Error(ctx, "cws: "+resp.Status)
		return false
	}
	woc.log.Info(ctx, "cws: successfully deleted workflow")
	return true
}

// submit the DAG of the workflow to the CWS scheduler
// NOTE: we (for now) only support workflows that have either:
//   - only one template definition (which is also the entry point)
//     i.e. no template invocations
//   - a single DAG template as the entry point that only calls template definitions
//     i.e. it calls no template invocations including itself
func (woc *wfOperationCtx) cwsSubmitDAG(ctx context.Context) bool {
	woc.log.Info(ctx, "cws: submitting DAG")
	entrypointUid := 0
	vertices := []vertex{{
		Label: "Entrypoint",
		Uid:   entrypointUid,
		Type:  "ORIGIN",
	}}
	edges := []edge{}

	var entrypointDag *v1alpha1.DAGTemplate

	for _, template := range woc.wf.Spec.Templates {
		templType := template.GetType()
		switch templType {
		case v1alpha1.TemplateTypeDAG:
			if template.Name != woc.wf.Spec.Entrypoint {
				woc.log.Error(ctx, "cws: DAG templates that are not the entrypoint are not supported with CWS")
				return false
			}
			entrypointDag = template.DAG
		case v1alpha1.TemplateTypeSteps:
			woc.log.Error(ctx, "cws: Steps templates are not supported with CWS")
			return false
		default:
		}
	}

	if entrypointDag != nil {
		if len(edges) > 0 {
			woc.log.Error(ctx, "cws: Did not expect second entrypoint when using DAG template as an entrypoint")
			return false
		}
		vertexUids := make(map[string]int)
		for _, vertex := range vertices {
			if vertex.Type != "PROCESS" {
				continue
			}
			vertexUids[vertex.Label] = vertex.Uid
			woc.log.WithFields(logging.Fields{
				"label": vertex.Label,
				"uid":   vertex.Uid,
			}).Error(ctx, "vertex mapping")
		}

		for _, task := range entrypointDag.Tasks {
			if task.Inline != nil {
				woc.log.Error(ctx, "cws: Inline templates are not supported with CWS")
				return false
			}
			vertexUid := len(vertices)
			taskVertex := vertex{
				Label: task.Name,
				Uid:   vertexUid,
				Type:  "PROCESS",
			}
			woc.log.WithField("vertex name", task.Name).Info(ctx, "cws: found vertex")
			vertices = append(vertices, taskVertex)
			vertexUids[task.Name] = vertexUid
		}

		for _, task := range entrypointDag.Tasks {
			taskUid := vertexUids[task.Name]
			if taskUid == 0 {
				woc.log.WithField("invalid template name", task.Template).Error(ctx, "cws: Expect DAG tasks to have valid template name")
				return false
			}
			if len(task.Dependencies) == 0 {
				edges = append(edges, edge{
					Label: "", // TODO: resonable naming?
					Uid:   len(edges),
					From:  entrypointUid,
					To:    taskUid,
				})
			}
			for _, dependency := range task.Dependencies {
				dependencyUid := vertexUids[dependency]
				if dependencyUid == 0 {
					woc.log.WithField("invalid name", dependency).
						Error(ctx, "cws: Expect DAG tasks to have non-invoker dependencies with valid names")
					return false
				}
				edges = append(edges, edge{
					Label: "", // TODO: resonable naming?
					Uid:   len(edges),
					From:  dependencyUid,
					To:    taskUid,
				})
			}
		}

	}

	jsonBytes, err := json.Marshal(vertices)
	if err != nil {
		woc.log.Error(ctx, "cws: "+err.Error())
		return false
	}
	jsonString := string(jsonBytes)
	woc.log.Info(ctx, "cws: "+jsonString)
	url := woc.globalParams[urlKey] + "/v1/scheduler/" + woc.cwsExecutionName() + "/DAG/vertices"
	resp, err := http.Post(url, "application/json", strings.NewReader(jsonString))
	if err != nil {
		woc.log.Error(ctx, "cws: "+err.Error())
		return false
	}
	response_body, err := io.ReadAll(resp.Body)
	woc.log.Info(ctx, "cws: "+string(response_body))
	if resp.StatusCode != 200 {
		woc.log.Error(ctx, "cws: "+resp.Status)
		return false
	}

	jsonBytes, err = json.Marshal(edges)
	if err != nil {
		woc.log.Error(ctx, "cws: "+err.Error())
		return false
	}
	jsonString = string(jsonBytes)
	woc.log.Info(ctx, "cws: "+jsonString)
	url = woc.globalParams[urlKey] + "/v1/scheduler/" + woc.cwsExecutionName() + "/DAG/edges"
	resp, err = http.Post(url, "application/json", strings.NewReader(jsonString))
	if err != nil {
		woc.log.Error(ctx, "cws: "+err.Error())
		return false
	}
	response_body, err = io.ReadAll(resp.Body)
	woc.log.Info(ctx, "cws: "+string(response_body))
	if resp.StatusCode != 200 {
		woc.log.Error(ctx, "cws: "+resp.Status)
		return false
	}

	woc.log.Info(ctx, "cws: successfully submitted DAG")
	return true

}

func (woc *wfOperationCtx) cwsStartBatch(ctx context.Context) bool {
	woc.log.Info(ctx, "cws: starting batch")
	client := &http.Client{}
	url := woc.globalParams[urlKey] + "/v1/scheduler/" + woc.cwsExecutionName() + "/startBatch"
	req, err := http.NewRequest("PUT", url, nil)
	resp, err := client.Do(req)
	if err != nil {
		woc.log.Error(ctx, "cws: "+err.Error())
		return false
	}
	if resp.StatusCode != 200 {
		woc.log.Error(ctx, "cws: "+resp.Status)
		return false
	}
	woc.log.Info(ctx, "cws: successfully started batch")
	tasksInBatch = 0
	return true
}

func (woc *wfOperationCtx) cwsEndBatch(ctx context.Context) bool {
	woc.log.Info(ctx, "cws: ending batch")
	client := &http.Client{}
	url := woc.globalParams[urlKey] + "/v1/scheduler/" + woc.cwsExecutionName() + "/endBatch"
	req, err := http.NewRequest("PUT", url, strings.NewReader(strconv.Itoa(tasksInBatch)))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		woc.log.Error(ctx, "cws: "+err.Error())
		return false
	}
	if resp.StatusCode != 200 {
		woc.log.Error(ctx, "cws: "+resp.Status)
		return false
	}
	woc.log.Info(ctx, "cws: successfully ended batch")
	return true
}

func getTaskName(node *v1alpha1.NodeStatus) string {
	s := node.DisplayName
	i := strings.Index(s, "(")
	if i != -1 {
		return s[:i]
	}
	return s
}

func (woc *wfOperationCtx) cwsRegisterTask(node *v1alpha1.NodeStatus, ctx context.Context) bool {
	woc.log.Info(ctx, "cws: registering task")
	// TODO: params and input task fields
	body := task{
		Task:            getTaskName(node),
		Name:            node.DisplayName,
		SchedulerParams: taskParams{},
		Inputs:          taskInputs{}, // NOTE: only file inputs matter
		RunName:         node.ID,
		Cpus:            0, // NOTE: never used by CWS scheduler
		MemoryInBytes:   0, // NOTE: never used by CWS scheduler
		WorkDir:         "/",
	}
	woc.log.WithFields(logging.Fields{
		"template name": node.TemplateName,
		"name":          node.Name,
		"display name":  node.DisplayName,
		"id":            node.ID,
	}).Info(ctx, "cws: NODE")
	woc.log.WithFields(logging.Fields{
		"task":     body.Task,
		"name":     body.Name,
		"run name": body.RunName,
	}).Info(ctx, "cws: BODY")
	jsonBytes, err := json.Marshal(body)
	if err != nil {
		woc.log.Error(ctx, "cws: json error - "+err.Error())
		return false
	}
	jsonString := string(jsonBytes)
	woc.log.Info(ctx, "cws: resulting string - "+jsonString)
	url := woc.globalParams[urlKey] + "/v1/scheduler/" + woc.cwsExecutionName() + "/task/"
	resp, err := http.Post(url+strconv.Itoa(registeredTasks), "application/json", strings.NewReader(jsonString))
	if err != nil {
		woc.log.Error(ctx, "cws: "+err.Error())
		return false
	}
	if resp.StatusCode != 200 {
		woc.log.Error(ctx, "cws: response "+resp.Status)
		return false
	}
	registeredTasks++
	woc.log.Info(ctx, "cws: successfully registered task")
	return true
}
