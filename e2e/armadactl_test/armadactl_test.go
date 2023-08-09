package armadactl_test

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/avast/retry-go"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/armadactl"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
	cq "github.com/armadaproject/armada/pkg/client/queue"
)

func TestVersion(t *testing.T) {
	buf := new(bytes.Buffer)
	app := &armadactl.App{
		Params: &armadactl.Params{},
		Out:    buf,
		Random: rand.Reader,
	}
	app.Params.QueueAPI = &armadactl.QueueAPI{}
	app.Params.ApiConnectionDetails = &client.ApiConnectionDetails{
		ArmadaUrl: "localhost:50051",
	}

	err := app.Version()
	require.NoError(t, err)

	out := buf.String()
	for _, s := range []string{"Commit", "Go version", "Built"} {
		require.True(t, strings.Contains(out, s), "expected output to contain %s, but got %s", s, out)
	}
}

func TestQueue(t *testing.T) {
	buf := new(bytes.Buffer)
	app := &armadactl.App{
		Params: &armadactl.Params{},
		Out:    buf,
		Random: rand.Reader,
	}
	app.Params.QueueAPI = &armadactl.QueueAPI{}
	app.Params.ApiConnectionDetails = &client.ApiConnectionDetails{
		ArmadaUrl: "localhost:50051",
	}

	// Setup the armadactl to use pkg/client as its backend for queue-related commands
	f := func() *client.ApiConnectionDetails {
		return app.Params.ApiConnectionDetails
	}
	app.Params.QueueAPI.Create = cq.Create(f)
	app.Params.QueueAPI.Delete = cq.Delete(f)
	app.Params.QueueAPI.GetInfo = cq.GetInfo(f)
	app.Params.QueueAPI.Update = cq.Update(f)

	// queue parameters
	priorityFactor := 1.337
	owners := []string{"ubar", "ubaz"}
	groups := []string{"gbar", "gbaz"}
	resourceLimits := map[string]float64{"cpu": 0.2, "memory": 0.9}

	// random queue name
	name, err := uuidString()
	require.NoError(t, err, "error creating UUID: string %s", err)

	queue, err := cq.NewQueue(&api.Queue{
		Name:           name,
		PriorityFactor: priorityFactor,
		UserOwners:     owners,
		GroupOwners:    groups,
		ResourceLimits: resourceLimits,
	})
	require.NoError(t, err, "failed to instantiate queue: %s", err)

	// create queue
	err = app.CreateQueue(queue)
	require.NoError(t, err)

	out := buf.String()
	buf.Reset()
	for _, s := range []string{fmt.Sprintf("Created queue %s\n", name)} {
		require.True(t, strings.Contains(out, s), "expected output to contain '%s', but got '%s'", s, out)
	}

	// describe
	err = app.DescribeQueue(name)
	require.NoError(t, err)

	out = buf.String()
	buf.Reset()
	for _, s := range []string{fmt.Sprintf("Queue: %s\n", name), "No queued or running jobs\n"} {
		require.True(t, strings.Contains(out, s), "expected output to contain '%s', but got '%s'", s, out)
	}

	// update
	err = app.UpdateQueue(queue)
	require.NoError(t, err)

	out = buf.String()
	buf.Reset()
	for _, s := range []string{fmt.Sprintf("Updated queue %s\n", name)} {
		if !strings.Contains(out, s) {
			t.Fatalf("expected output to contain '%s', but got '%s'", s, out)
		}
	}

	// delete
	err = app.DeleteQueue(name)
	require.NoError(t, err)

	out = buf.String()
	buf.Reset()
	for _, s := range []string{"Deleted", name, "\n"} {
		require.True(t, strings.Contains(out, s), "expected output to contain '%s', but got '%s'", s, out)
	}

	// TODO armadactl returns empty output for non-existing queues
	// // request details about the queue
	// err = app.DescribeQueue(name)
	// if err == nil {
	// 	t.Fatal("expected an error, but got none")
	// }

	// // change queue details
	// err = app.UpdateQueue(name, priorityFactor, owners, groups, resourceLimits)
	// if err == nil {
	// 	t.Fatal("expected an error, but got none")
	// }
}

func TestJob(t *testing.T) {
	buf := new(bytes.Buffer)
	app := &armadactl.App{
		Params: &armadactl.Params{},
		Out:    buf,
		Random: rand.Reader,
	}
	app.Params.QueueAPI = &armadactl.QueueAPI{}
	app.Params.ApiConnectionDetails = &client.ApiConnectionDetails{
		ArmadaUrl: "localhost:50051",
	}

	// Setup the armadactl to use pkg/client as its backend for queue-related commands
	f := func() *client.ApiConnectionDetails {
		return app.Params.ApiConnectionDetails
	}
	app.Params.QueueAPI.Create = cq.Create(f)
	app.Params.QueueAPI.Delete = cq.Delete(f)
	app.Params.QueueAPI.GetInfo = cq.GetInfo(f)
	app.Params.QueueAPI.Update = cq.Update(f)

	// queue parameters
	priorityFactor := 1.337
	owners := []string{"ubar", "ubaz"}
	groups := []string{"gbar", "gbaz"}
	resourceLimits := map[string]float64{"cpu": 0.2, "memory": 0.9}

	// random queue name
	name, err := uuidString()
	require.NoError(t, err, "error creating UUID: string %s", err)

	// job parameters
	jobData := []byte(fmt.Sprintf(`
queue: %s
jobSetId: set1
jobs:
  - priority: 1
    namespace: personal-anonymous
    podSpec:
      terminationGracePeriodSeconds: 0
      restartPolicy: Never
      containers:
        - name: ls
          imagePullPolicy: IfNotPresent
          image: alpine:3.18
          command:
            - sh
            - -c
          args:
            - ls
          resources:
            limits:
              memory: 100Mi
              cpu: 1
            requests:
              memory: 100Mi
              cpu: 1`, name))
	jobDir := t.TempDir()
	jobFile, err := os.CreateTemp(jobDir, "test")
	require.NoError(t, err, "error creating jobfile")

	jobPath := jobFile.Name()
	_, err = jobFile.Write(jobData)
	require.NoError(t, err)

	err = jobFile.Sync()
	require.NoError(t, err)

	err = jobFile.Close()
	require.NoError(t, err)

	queue, err := cq.NewQueue(&api.Queue{
		Name:           name,
		PriorityFactor: priorityFactor,
		ResourceLimits: resourceLimits,
		UserOwners:     owners,
		GroupOwners:    groups,
	})
	require.NoError(t, err, "failed to instantiate queue: %s", err)

	// create a queue to use for the tests
	err = app.CreateQueue(queue)
	require.NoError(t, err, "error creating test queue: %s", err)
	buf.Reset()

	// submit
	err = app.Submit(jobPath, false)
	require.NoError(t, err)

	out := buf.String()
	buf.Reset()
	for _, s := range []string{"Submitted job with id", "to job set set1\n"} {
		require.True(t, strings.Contains(out, s), "expected output to contain '%s', but got '%s'", s, out)
	}

	// analyze
	err = retry.Do(
		func() error {
			err = app.Analyze(name, "set1")
			if err != nil {
				return fmt.Errorf("expected no error, but got %s", err)
			}

			out = buf.String()
			buf.Reset()

			if strings.Contains(out, "Found no events associated") {
				return fmt.Errorf("no events found, got response %s", out)
			}

			for _, s := range []string{fmt.Sprintf("Querying queue %s for job set set1", name), "api.JobSubmittedEvent", "api.JobQueuedEvent"} {
				if !strings.Contains(out, s) {
					return fmt.Errorf("expected output to contain '%s', but got '%s'", s, out)
				}
			}

			return nil
		},
		retry.Attempts(100), // default retry delay is 100ms and it may take 10 seconds for the server to commit a job
	)
	require.NoError(t, err, "error on calling analyze")
	// resources
	// no need for retry since we can be sure the job has been committed to the db at this point
	err = app.Resources(name, "set1")
	require.NoError(t, err)

	out = buf.String()
	buf.Reset()
	for _, s := range []string{"Job ID:", "maximum used resources:", "\n"} {
		require.True(t, strings.Contains(out, s))
	}

	// reprioritize
	err = app.Reprioritize("", name, "set1", 2)
	require.NoError(t, err)

	out = buf.String()
	buf.Reset()
	for _, s := range []string{"Reprioritized jobs with ID:\n"} {
		require.True(t, strings.Contains(out, s))
	}

	// cancel
	err = app.Cancel(name, "set1", "")
	require.NoError(t, err)

	out = buf.String()
	buf.Reset()
	for _, s := range []string{"Requested cancellation for jobs", "\n"} {
		require.True(t, strings.Contains(out, s))
	}
}

// uuidString returns a randomly generated UUID as a string.
func uuidString() (string, error) {
	uuid, err := uuid.NewUUID()
	if err != nil {
		return "", fmt.Errorf("[uuidString] error creating UUID: %s", err)
	}
	return uuid.String(), nil
}
