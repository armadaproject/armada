package queue

import (
	"fmt"
	"io"
	"testing"
	"testing/quick"
)

func TestDelete(t *testing.T) {
	properties := map[string]interface{}{
		"invalidArguments": func(manyArguments bool) bool {
			deleteQueue := func(queueName string) error {
				return nil
			}

			cmd := Delete(deleteQueue)
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
			deleteQueue := func(queueName string) error {
				return fmt.Errorf("failed to delete queue")
			}

			cmd := Delete(deleteQueue)
			cmd.SetArgs([]string{queueName})
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)

			if err := cmd.Execute(); err == nil {
				t.Errorf("failed to error")
				return false
			}

			return true
		},
		"success": func(queueName string) bool {
			deleteQueue := func(name string) error {
				if queueName != name {
					return fmt.Errorf("invalid queueName")
				}
				return nil
			}

			cmd := Delete(deleteQueue)
			cmd.SetArgs([]string{queueName})
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)

			if err := cmd.Execute(); err != nil {
				t.Errorf("failed to delete queue: %s", err)
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
