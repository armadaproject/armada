package queue

import (
	"fmt"
	"io"
	"testing"
	"testing/quick"

	"github.com/G-Research/armada/pkg/api"
)

func TestDescribe(t *testing.T) {
	properties := map[string]interface{}{
		"invalidArguments": func(manyArguments bool) bool {
			getQueueInfo := func(queueName string) (*api.QueueInfo, error) {
				return nil, nil
			}

			cmd := Describe(getQueueInfo)
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)

			if manyArguments {
				cmd.SetArgs([]string{"arg1", "arg2", "arg3"})
			}

			if err := cmd.Execute(); err == nil {
				t.Errorf("failed to handle invalid arguments")
				return false
			}

			return true
		},
		"errorHandling": func(queueName string) bool {
			getQueueInfo := func(queueName string) (*api.QueueInfo, error) {
				return nil, fmt.Errorf("failed to retrieve queue info")
			}

			cmd := Describe(getQueueInfo)
			cmd.SetArgs([]string{queueName})
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)

			if err := cmd.Execute(); err == nil {
				t.Errorf("failed to error")
				return false
			}

			return true
		},
		"sucess": func(queueName string, jobSetInfos []api.JobSetInfo) bool {
			describeQueue := func(name string) (*api.QueueInfo, error) {
				if queueName != name {
					return nil, fmt.Errorf("invalid queueName")
				}

				queueInfo := api.QueueInfo{
					Name:          queueName,
					ActiveJobSets: make([]*api.JobSetInfo, len(jobSetInfos)),
				}

				for index, info := range jobSetInfos {
					queueInfo.ActiveJobSets[index] = &info
				}

				return &queueInfo, nil

			}

			cmd := Describe(describeQueue)
			cmd.SetArgs([]string{queueName})
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)

			if err := cmd.Execute(); err != nil {
				t.Errorf("failed to describe queue: %s", err)
				return false
			}

			return true
		},
	}

	for name, property := range properties {
		t.Run(name, func(tp *testing.T) {
			if err := quick.Check(property, nil); err != nil {
				tp.Error(err)
			}
		})
	}
}
