package joblogger

import (
	"bytes"
	"fmt"
)

func (srv *JobLogger) PrintLogs() {
	podLogMap := srv.Logs()
	_, _ = fmt.Fprintf(srv.out, "\nArmada JobSet %s logs in queue %s\n", srv.jobSetId, srv.queue)
	for pod, logs := range podLogMap {
		_, _ = fmt.Fprintf(srv.out, "Pod %s logs:\n", pod)
		_, _ = fmt.Fprintf(srv.out, "%s\n", logs.String())
	}
}

func (srv *JobLogger) Logs() map[string]*bytes.Buffer {
	podLogMap := make(map[string]*bytes.Buffer)
	srv.podMap.Range(func(k, v interface{}) bool {
		key, _ := k.(string)
		info, _ := v.(*podInfo)
		podLogMap[key] = info.Logs
		return true
	})
	return podLogMap
}
