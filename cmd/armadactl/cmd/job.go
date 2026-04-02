package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/armadaproject/armada/pkg/api"
	introspectionapi "github.com/armadaproject/armada/pkg/api/introspection"
)

var jobAddr string
var jobTimeout int
var jobIncludeEvents bool

var (
      styleRunning  = lipgloss.NewStyle().Foreground(lipgloss.Color("33"))  // blue
      styleSuccess  = lipgloss.NewStyle().Foreground(lipgloss.Color("2"))   // green
      styleFailed   = lipgloss.NewStyle().Foreground(lipgloss.Color("1"))   // red
      styleWarning  = lipgloss.NewStyle().Foreground(lipgloss.Color("3"))   // yellow
      styleDim      = lipgloss.NewStyle().Foreground(lipgloss.Color("8"))   // grey
)

func jobCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use: "job <job-id>",
		Short: "Show status summary for a job (pod, node, errors)",
		Args: cobra.ExactArgs(1),
		RunE: runJob,
	}

	cmd.Flags().StringVar(&jobAddr, "addr", "", "armada api address (env ARMADA_API_ADDR used if empty)")
	cmd.Flags().IntVar(&jobTimeout, "timeout", 15, "rpc timeout seconds")
	cmd.Flags().BoolVar(&jobIncludeEvents, "events", false, "incldue pod events in output")

	return cmd
}

func runJob(cmd *cobra.Command, args []string) error {
	jobID := args[0]
	addr := jobAddr

	if addr == "" {
		addr = os.Getenv("ARMADA_API_ADDR")
	}
	if addr == "" {
		if v, err := cmd.Flags().GetString("armadaUrl"); err == nil && v != "" {
			addr = v
		}
		if addr == "" {
			if v, err := cmd.InheritedFlags().GetString("armadaUrl"); err == nil && v != "" {
				addr = v
			}
		}
	}
	if addr == "" {
		addr = "localhost:50061"
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(jobTimeout)*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}
	defer conn.Close()

	jobsClient := api.NewJobsClient(conn)
	introspectionClient := introspectionapi.NewIntrospectionClient(conn)

	jobDetailsResp, err := jobsClient.GetJobDetails(ctx, &api.JobDetailsRequest{
		JobIds: []string{jobID},
		ExpandJobRun: true,
	})
	if err != nil {
		return fmt.Errorf("GetJobDetails: %w", err)
	}
	jobDetails := jobDetailsResp.GetJobDetails()[jobID]
	if jobDetails == nil {
		return fmt.Errorf("job %s not found", jobID)
	}

	type podResult struct {
		resp *introspectionapi.DescribeJobPodResponse
		err error
	}

	type nodeResult struct {
		resp *introspectionapi.DescribeNodeResponse
		err error
	}

	var wg sync.WaitGroup
	podCh := make(chan podResult, 1)
	nodeCh := make(chan nodeResult, 1)
	
	wg.Add(2)
	go func() {
		defer wg.Done()
		resp, err := introspectionClient.KubeDescribeJobPod(ctx, &introspectionapi.DescribeJobPodRequest{
			JobId: jobID,
			IncludeEvents: jobIncludeEvents,
		})
		podCh <- podResult{resp, err}
	}()
	go func() {
		defer wg.Done()
		resp, err := introspectionClient.KubeDescribeNodeByJobId(ctx, &introspectionapi.DescribeNodeByJobIdRequest{
			JobId: jobID,
		})
		nodeCh <- nodeResult{resp, err}
	}()
	wg.Wait()

	podRes := <-podCh
	nodeRes := <-nodeCh

	pf := func(label, value string) {
		fmt.Printf("  %-13s %s\n", label+":", value)
	}

	// Job level info
	pf("jobState", colorJobState(jobDetails.GetState(), jobStateString(jobDetails.GetState())))
	if s := summarizeJob(jobDetails, podRes.resp, nodeRes.resp); s != "" {
		pf("summary", s)
	}
	pf("queue", jobDetails.GetQueue())
	pf("namespace", jobDetails.GetNamespace())

	latestRun := latestJobRun(jobDetails)
	if latestRun != nil {
		pf("runID", latestRun.GetRunId())
		pf("cluster", latestRun.GetCluster())
		pf("node", latestRun.GetNode())
	}

	// Pod info
	fmt.Println()
	if podRes.err != nil {
		pf("podStatus", fmt.Sprintf("(unavailable: %v)", podRes.err))
	} else {
		pod := podRes.resp
		pf("podName", pod.GetPodName())
		pf("podPhase", pod.GetPhase())
	}

	// Node info
	if nodeRes.err != nil {
		pf("nodeStatus", fmt.Sprintf("(unavailable: %v)", nodeRes.err))
	} else {
		pf("nodeStatus", colorNodeStatus(nodeReadyStatus(nodeRes.resp)))
	}

	// Errors
	fmt.Println()
	if podRes.err == nil {
		errors := collectPodErrors(podRes.resp)
		if len(errors) > 0 {
			fmt.Printf("  %-13s\n", "podErrors:")
			for _, e := range errors {
				fmt.Printf("    %s\n", e)
			}
		}
	}

	// Container statuses
	if podRes.err == nil && len(podRes.resp.GetContainers()) > 0 {
		fmt.Println()
		fmt.Printf("  %-13s\n", "containers:")
		for _, c := range podRes.resp.GetContainers() {
			ready := "false"
			if c.GetReady() {
				ready = "true"
			}
			line := fmt.Sprintf("    [%s]  state=%-12s  ready=%s  restarts=%d",
				c.GetName(), c.GetState(), ready, c.GetRestartCount())
			if c.GetReason() != "" {
				line += fmt.Sprintf("  reason=%s", c.GetReason())
			}
			if c.GetMessage() != "" {
				line += fmt.Sprintf("  message=%s", c.GetMessage())
			}
			fmt.Println(line)
		}
	}

	// Events
	if jobIncludeEvents && podRes.err == nil {
		events := podRes.resp.GetEvents()
		fmt.Println()
		if len(events) == 0 {
			pf("events", "None")
		} else {
			fmt.Printf("  %-13s\n", "events:")
			for _, ev := range events {
				ts := ev.GetLastTimestamp()
				if ts == "" {
					ts = ev.GetFirstTimestamp()
				}
				ago := formatAgo(ts)
				fmt.Printf("    [%s] %s: %s %s%s\n",
					ev.GetType(), ev.GetReason(), ev.GetMessage(), ts, ago)
			}
		}
	}

	return nil
}

func summarizeJob(jobDetails *api.JobDetails, pod *introspectionapi.DescribeJobPodResponse, node *introspectionapi.DescribeNodeResponse) string {
	// Job-level terminal states that don't need pod/node info.
	switch jobDetails.GetState() {
	case api.JobState_REJECTED:
		if r := jobDetails.GetCancelReason(); r != "" {
			return "Job was rejected: " + r
		}
		return "Job was rejected"
	case api.JobState_CANCELLED:
		return "Job was cancelled"
	case api.JobState_PREEMPTED:
		return "Job was preempted"
	case api.JobState_SUCCEEDED:
		return "Job completed successfully"
	}

	// Run-level: lease expired is a strong node-failure signal stored in DB.
	if run := latestJobRun(jobDetails); run != nil {
		if run.GetState() == api.JobRunState_RUN_STATE_LEASE_EXPIRED {
			return "The node running this job became unavailable"
		}
	}

	if pod != nil {
		// Container-level signals, most specific first.
		for _, c := range pod.GetContainers() {
			switch strings.ToLower(c.GetReason()) {
			case "oomkilled":
				return fmt.Sprintf("Job ran out of memory (OOMKilled in container %q)", c.GetName())
			case "deadlineexceeded":
				return "Job exceeded its time limit"
			case "imagepullbackoff", "errimagepull", "invalidimagename":
				return fmt.Sprintf("Could not download the job's container image (container %q)", c.GetName())
			case "createcontainerconfigerror", "createcontainererror":
				return fmt.Sprintf("Job has a missing or invalid configuration (container %q)", c.GetName())
			case "crashloopbackoff":
				return fmt.Sprintf("Job exited with an error (container %q crash-looping, restarted %d times)", c.GetName(), c.GetRestartCount())
			case "podinitializing":
				return "Setup steps are running"
			case "containercreating":
				return "Downloading container image"
			}
		}

		// Pod scheduling conditions.
		for _, cond := range pod.GetConditions() {
			if cond.GetType() == "PodScheduled" && cond.GetStatus() == "False" {
				msg := cond.GetMessage()
				msgLower := strings.ToLower(msg)
				switch {
				case strings.Contains(msgLower, "insufficient"):
					return "Waiting for CPU / memory / GPU to become available"
				case strings.Contains(msgLower, "node selector"),
					strings.Contains(msgLower, "node affinity"),
					strings.Contains(msgLower, "taint"):
					return "No node matches this job's requirements"
				default:
					if msg != "" {
						return "Job is unschedulable: " + msg
					}
					return "Job is unschedulable"
				}
			}
		}

		// Generic terminated container fallback.
		for _, c := range pod.GetContainers() {
			if strings.EqualFold(c.GetState(), "terminated") &&
				c.GetReason() != "" &&
				!strings.EqualFold(c.GetReason(), "Completed") {
				return fmt.Sprintf("Job exited with an error (container %q: %s)", c.GetName(), c.GetReason())
			}
		}
	}

	// Node condition: less reliable than run state since it reflects current state.
	if node != nil && jobDetails.GetState() == api.JobState_FAILED {
		for _, cond := range node.GetConditions() {
			if cond.GetType() == "Ready" && cond.GetStatus() != "True" {
				return "The node running this job became unavailable"
			}
		}
	}

	// State-based fallbacks.
	switch jobDetails.GetState() {
	case api.JobState_RUNNING:
		return "Job is running"
	case api.JobState_PENDING, api.JobState_QUEUED, api.JobState_LEASED, api.JobState_SUBMITTED:
		return "Job is pending"
	}

	return ""
}

func jobStateString(s api.JobState) string {
	name := s.String()
	name = strings.TrimPrefix(name, "JOB_STATE_")
	return strings.Title(strings.ToLower(name))
}

func latestJobRun(jd *api.JobDetails) *api.JobRunDetails {
	if jd == nil {
		return nil
	}
	runs := jd.GetJobRuns()
	if len(runs) == 0 {
		return nil
	}
	latestID := jd.GetLatestRunId()
	for _, r := range runs {
		if r.GetRunId() == latestID {
			return r
		}
	}
	return runs[len(runs)-1]
}

func nodeReadyStatus(node *introspectionapi.DescribeNodeResponse) string {
	if node == nil {
		return "Unknown"
	}
	for _, cond := range node.GetConditions() {
		if cond.GetType() == "Ready" {
			if cond.GetStatus() == "True" {
				return "Ready"
			}
			reason := cond.GetReason()
			if reason != "" {
				return "NotReady (" + reason + ")"
			}
			return "NotReady"
		}
	}
	return "Unknown"
}

func collectPodErrors(pod *introspectionapi.DescribeJobPodResponse) []string {
	var errs []string

	for _, c := range pod.GetContainers() {
		if strings.EqualFold(c.GetState(), "terminated") || strings.EqualFold(c.GetState(), "waiting") {
			if c.GetReason() != "" && !strings.EqualFold(c.GetReason(), "Completed") {
				msg := fmt.Sprintf("[container/%s] %s: %s", c.GetName(), c.GetReason(), c.GetMessage())
				errs = append(errs, strings.TrimRight(msg, ": "))
			}
		}
	}

	for _, cond := range pod.GetConditions() {
		if cond.GetStatus() == "False" && cond.GetReason() != "" &&
			!strings.EqualFold(cond.GetReason(), "PodCompleted") {
			msg := fmt.Sprintf("[condition/%s] %s: %s", cond.GetType(), cond.GetReason(), cond.GetMessage())
			errs = append(errs, strings.TrimRight(msg, ": "))
		}
	}

	return errs
}

func formatAgo(ts string) string {
	if ts == "" {
		return ""
	}

	// try common formats
	formats := []string{
		time.RFC3339,
		"2006-01-02 15:04:05 -0700 MST",
		"2006-01-02T15:04:05Z",
	}

	var t time.Time
	var err error

	for _, f := range formats {
		t, err = time.Parse(f, ts)
		if err == nil {
			break
		}
	}
	if err != nil {
		return ""
	}

	d := time.Since(t)
	switch {
	case d < 2*time.Minute:
		return fmt.Sprintf(" (%ds ago)", int(d.Seconds()))
	case d < 2*time.Hour:
		return fmt.Sprintf(" (%d mins ago)", int(d.Minutes()))
    default:
		return fmt.Sprintf(" (%dh ago)", int(d.Hours()))

	}
}

func colorJobState(s api.JobState, label string) string {
      switch s {
      case api.JobState_RUNNING:
          return styleRunning.Render(label)
      case api.JobState_SUCCEEDED:
          return styleSuccess.Render(label)
      case api.JobState_FAILED:
          return styleFailed.Render(label)
      case api.JobState_CANCELLED, api.JobState_PREEMPTED:
          return styleWarning.Render(label)
      case api.JobState_QUEUED, api.JobState_PENDING, api.JobState_LEASED, api.JobState_SUBMITTED:
          return styleDim.Render(label)
      default:
          return label
      }
  }

  func colorNodeStatus(status string) string {
      if strings.HasPrefix(status, "Ready") {
          return styleSuccess.Render(status)
      }
      if strings.HasPrefix(status, "NotReady") {
          return styleFailed.Render(status)
      }
      return status
  }