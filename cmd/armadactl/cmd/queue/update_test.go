package queue

import (
	"fmt"
	"io"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/G-Research/armada/pkg/api"
)

func TestUpdate(t *testing.T) {
	type flags struct {
		PriorityFactor bool
		Owners         bool
		GroupOwners    bool
		ResourceLimits bool
	}

	properties := map[string]interface{}{
		"validFlags": func(flags flags) bool {
			updateQueue := func(q api.Queue) error {
				switch {
				case q.Name != "queue_name":
					return fmt.Errorf("invalid queue name: %s, expected: queue_name", q.Name)
				case flags.PriorityFactor && q.PriorityFactor != 0.1:
					return fmt.Errorf("invalid priority factor")
				case flags.Owners && !reflect.DeepEqual(q.UserOwners, []string{"user1", "user2"}):
					return fmt.Errorf("invalid user owners")
				case flags.GroupOwners && !reflect.DeepEqual(q.GroupOwners, []string{"group1", "group2"}):
					return fmt.Errorf("invalid group owners")
				case flags.ResourceLimits && !reflect.DeepEqual(q.ResourceLimits, map[string]float64{"cpu": 0.3, "memory": 0.2}):
					return fmt.Errorf("invalid resource limits")
				default:
					return nil
				}
			}

			cmd := Update(updateQueue)
			if flags.PriorityFactor {
				cmd.Flags().Set("priorityFactor", "0.1")
			}
			if flags.Owners {
				cmd.Flags().Set("owners", "user1,user2")
			}
			if flags.GroupOwners {
				cmd.Flags().Set("groupOwners", "group1,group2")
			}
			if flags.ResourceLimits {
				cmd.Flags().Set("resourceLimits", "cpu=0.3,memory=0.2")
			}
			cmd.SetArgs([]string{"queue_name"})

			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)

			if err := cmd.Execute(); err != nil {
				t.Errorf("failed to execute command: %s", err)
				return false
			}
			return true
		},
		"invalidFlags": func(flags flags) bool {
			updateQueue := func(q api.Queue) error {
				return nil
			}

			args := []string{}
			cmd := Update(updateQueue)
			if flags.PriorityFactor {
				args = append(args, "--priorityFactor", "not_a_float")
			}
			if flags.Owners {
				args = append(args, "--owners", "not a string array")
			}
			if flags.GroupOwners {
				args = append(args, "--groupOwners", "not a string array")
			}
			if flags.ResourceLimits {
				args = append(args, "--resourceLimits", "not a string map")
			}

			isError := flags.ResourceLimits || flags.Owners || flags.GroupOwners || flags.PriorityFactor
			cmd.SetArgs(args)
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)

			if err := cmd.Execute(); err == nil && isError {
				t.Error("failed handle flag validation error")
				return false
			}

			return true
		},
		"invalidArguments": func(manyArguments bool) bool {
			updateQueue := func(q api.Queue) error {
				return nil
			}

			cmd := Update(updateQueue)
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)

			// Should fail on number of arguments != 1
			if manyArguments {
				cmd.SetArgs([]string{"arg1", "arg2"})
			}

			if err := cmd.Execute(); err == nil {
				t.Error("failed handle invalid number of arguments")
				return false
			}
			return true
		},
		"errorHandling": func(queueName string) bool {
			updateQueue := func(q api.Queue) error {
				return fmt.Errorf("failed to Update queue")
			}

			cmd := Update(updateQueue)
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)
			cmd.SetArgs([]string{"queue_name"})

			if err := cmd.Execute(); err == nil {
				t.Error("failed to handle error")
				return false
			}

			return true
		},
	}

	for name, property := range properties {
		t.Run(name, func(pt *testing.T) {
			if err := quick.Check(property, nil); err != nil {
				pt.Fatal(err)
			}
		})
	}
}
