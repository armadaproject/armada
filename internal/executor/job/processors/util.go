package processors

import (
	"github.com/armadaproject/armada/internal/executor/job"
	"github.com/armadaproject/armada/internal/executor/util"
	v1 "k8s.io/api/core/v1"
)

type runPodInfo struct {
	Run *job.RunState
	Pod *v1.Pod
}

func createRunPodInfos(runs []*job.RunState, pods []*v1.Pod) []*runPodInfo {
	runIdsToPod := make(map[string]*v1.Pod, len(pods))
	for _, pod := range pods {
		runId := util.ExtractJobRunId(pod)
		if runId != "" {
			runIdsToPod[runId] = pod
		}
	}

	result := make([]*runPodInfo, 0, len(runs))
	for _, run := range runs {
		pod := runIdsToPod[run.Meta.RunId]
		result = append(result, &runPodInfo{Run: run, Pod: pod})
	}
	return result
}
