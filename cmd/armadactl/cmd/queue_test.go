package cmd

/*
This file tests command-line arguments and flags are passed through correctly (e.g., in the right
order) to the underlying API of armadactl, which, during normal operation, is the Armada client
package.

It does so by hijacking the armadactl setup process and replacing the PreRunE function of the Cobra
"create queue" command, which initialises the armadactl app, with a function that replaces the
regular queue API of armadactl with a version that compares against hard-coded correct values.

*/

import (
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/G-Research/armada/internal/armadactl"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client/queue"
)

// Used for in-line initialization of pointers to floats
func makeFloat64Pointer(v float64) *float64 {
	return &v
}

type flag struct {
	name  string
	value string
}

func TestCreate(t *testing.T) {
	// TODO there are no tests for invalid input because cobra silently discards those inputs without raising errors
	tests := map[string]struct {
		Flags          []flag
		PriorityFactor *float64
		Owners         []string
		GroupOwners    []string
		ResourceLimits map[string]float64
		err            error // expected error, or nil if no error is expected
	}{
		"default flags":         {nil, nil, nil, nil, nil, nil},
		"valid priority":        {[]flag{{"priorityFactor", "1.0"}}, makeFloat64Pointer(1.0), nil, nil, nil, nil},
		"valid owners":          {[]flag{{"owners", "user1,user2"}}, nil, []string{"user1", "user2"}, nil, nil, nil},
		"valid group owners":    {[]flag{{"groupOwners", "group1,group2"}}, nil, nil, []string{"group1", "group2"}, nil, nil},
		"valid resource limits": {[]flag{{"resourceLimits", "cpu=0.3,memory=0.2"}}, nil, nil, nil, map[string]float64{"cpu": 0.3, "memory": 0.2}, nil},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			// Create app object, cobra command, and hijack the app setup process to insert a
			// function that does validation
			a := armadactl.New()
			cmd := queueCreateCmdWithApp(a)
			cmd.PreRunE = func(cmd *cobra.Command, args []string) error {
				a.Params.QueueAPI.Create = func(q queue.Queue) error {
					a.Out = io.Discard
					permissions := []queue.Permissions{
						{
							Subjects: queue.NewPermissionSubjectsFromOwners(test.Owners, test.GroupOwners),
							Verbs:    queue.AllPermissionVerbs(),
						},
					}

					// Check that the arguments passed into the API are equal to those provided via CLI flags
					if q.Name != "arbitrary" {
						t.Fatalf("expected Name to be 'arbitrary', but got %s", q.Name)
					}
					if test.PriorityFactor != nil && float64(q.PriorityFactor) != *test.PriorityFactor {
						t.Fatalf("expected PriorityFactor to be %v, but got %v", *test.PriorityFactor, q.PriorityFactor)
					}
					if test.Owners != nil && !reflect.DeepEqual(q.Permissions, permissions) {
						t.Fatalf("expected Permissions to be %#v, but got %#v", permissions, q.Permissions)
					}

					if test.ResourceLimits != nil {
						for resourceName, resourceLimit := range q.ResourceLimits {
							if test.ResourceLimits[string(resourceName)] != float64(resourceLimit) {
								t.Fatalf("invalid resource limit: [%s]%f expected: [%s]%f", resourceName, test.ResourceLimits[string(resourceName)], resourceName, resourceLimit)
							}
						}
					}
					return nil
				}
				return nil
			}

			// Arbitrary queue name
			cmd.SetArgs([]string{"arbitrary"})

			// Set CLI flags; falls back to default values if not set
			for _, flag := range test.Flags {
				cmd.Flags().Set(flag.name, flag.value)
			}

			// Execute the command and check any error
			if err := cmd.Execute(); err != test.err {
				t.Fatalf("command failed with an unexpected error: %s", err)
			}
		})
	}
}

func TestDelete(t *testing.T) {
	// Create app object, cobra command, and hijack the app setup process to insert a
	// function that does validation
	a := armadactl.New()
	cmd := queueDeleteCmdWithApp(a)
	cmd.PreRunE = func(cmd *cobra.Command, args []string) error {
		a.Params.QueueAPI.Delete = func(name string) error {
			a.Out = io.Discard

			// Check that the arguments passed into the API are equal to those provided via CLI flags
			if name != "arbitrary" {
				t.Fatalf("expected Name to be 'arbitrary', but got %s", name)
			}
			return nil
		}
		return nil
	}

	// Arbitrary queue name
	cmd.SetArgs([]string{"arbitrary"})

	// Execute the command and check any error
	if err := cmd.Execute(); err != nil && !strings.Contains(err.Error(), "expected test error") {
		t.Fatalf("command failed with an unexpected error: %s", err)
	}
}

func TestDescribe(t *testing.T) {
	// Create app object, cobra command, and hijack the app setup process to insert a
	// function that does validation
	a := armadactl.New()
	cmd := queueDescribeCmdWithApp(a)
	cmd.PreRunE = func(cmd *cobra.Command, args []string) error {
		a.Params.QueueAPI.GetInfo = func(name string) (*api.QueueInfo, error) {
			a.Out = io.Discard

			// Check that the arguments passed into the API are equal to those provided via CLI flags
			if name != "arbitrary" {
				t.Fatalf("expected Name to be 'arbitrary', but got %s", name)
			}
			return nil, fmt.Errorf("expected test error to force armadactl.DescribeQueue to return")
		}
		return nil
	}

	// Arbitrary queue name
	cmd.SetArgs([]string{"arbitrary"})

	// Execute the command and check any error
	if err := cmd.Execute(); err != nil && !strings.Contains(err.Error(), "expected test error") {
		t.Fatalf("command failed with an unexpected error: %s", err)
	}
}

func TestUpdate(t *testing.T) {
	// TODO there are no tests for invalid input because cobra silently discards those inputs without raising errors
	tests := map[string]struct {
		Flags          []flag
		PriorityFactor *float64
		Owners         []string
		GroupOwners    []string
		ResourceLimits map[string]float64
		err            error // expected error, or nil if no error is expected
	}{
		"default flags":         {nil, nil, nil, nil, nil, nil},
		"valid priority":        {[]flag{{"priorityFactor", "1.0"}}, makeFloat64Pointer(1.0), nil, nil, nil, nil},
		"valid owners":          {[]flag{{"owners", "user1,user2"}}, nil, []string{"user1", "user2"}, nil, nil, nil},
		"valid group owners":    {[]flag{{"groupOwners", "group1,group2"}}, nil, nil, []string{"group1", "group2"}, nil, nil},
		"valid resource limits": {[]flag{{"resourceLimits", "cpu=0.3,memory=0.2"}}, nil, nil, nil, map[string]float64{"cpu": 0.3, "memory": 0.2}, nil},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			// Create app object, cobra command, and hijack the app setup process to insert a
			// function that does validation
			a := armadactl.New()
			cmd := queueUpdateCmdWithApp(a)
			cmd.PreRunE = func(cmd *cobra.Command, args []string) error {
				a.Params.QueueAPI.Update = func(q queue.Queue) error {
					permissions := []queue.Permissions{
						{
							Subjects: queue.NewPermissionSubjectsFromOwners(test.Owners, test.GroupOwners),
							Verbs:    queue.AllPermissionVerbs(),
						},
					}

					// Check that the arguments passed into the API are equal to those provided via CLI flags
					if q.Name != "arbitrary" {
						t.Fatalf("expected Name to be 'arbitrary', but got %s", q.Name)
					}
					if test.PriorityFactor != nil && float64(q.PriorityFactor) != *test.PriorityFactor {
						t.Fatalf("expected PriorityFactor to be %v, but got %v", *test.PriorityFactor, q.PriorityFactor)
					}
					if test.Owners != nil && !reflect.DeepEqual(q.Permissions, permissions) {
						t.Fatalf("expected Permissions to be %#v, but got %#v", permissions, q.Permissions)
					}

					if test.ResourceLimits != nil {
						for resourceName, resourceLimit := range q.ResourceLimits {
							if test.ResourceLimits[string(resourceName)] != float64(resourceLimit) {
								t.Fatalf("invalid resource limit: [%s]%f expected: [%s]%f", resourceName, test.ResourceLimits[string(resourceName)], resourceName, resourceLimit)
							}
						}
					}
					return nil
				}
				return nil
			}

			// Arbitrary queue name
			cmd.SetArgs([]string{"arbitrary"})

			// Set CLI flags; falls back to default values if not set
			for _, flag := range test.Flags {
				cmd.Flags().Set(flag.name, flag.value)
			}

			// Execute the command and check any error
			if err := cmd.Execute(); err != test.err {
				t.Fatalf("command failed with an unexpected error: %s", err)
			}
		})
	}
}
