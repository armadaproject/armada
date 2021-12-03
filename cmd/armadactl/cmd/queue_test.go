package cmd

/*
This file tests command-line arguments and flags are passed through correctly (e.g., in the right
order) to the underlying API of armadactl, which, during normal operation, is the Armada client
package.

It does so by hijacking the armadactl setup process and replacing the PreRunE function of the Cobra
"create queue" command, which initializes the armadactl app, with a function that replaces the
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
		"valid priority":        {[]flag{{"priorityFactor", "0.1"}}, makeFloat64Pointer(0.1), nil, nil, nil, nil},
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
				a.Params.QueueAPI.Create = func(queue api.Queue) error {
					a.Out = io.Discard

					// Check that the arguments passed into the API are equal to those provided via CLI flags
					if queue.Name != "arbitrary" {
						t.Fatalf("expected Name to be 'arbitrary', but got %s", queue.Name)
					}
					if test.PriorityFactor != nil && queue.PriorityFactor != *test.PriorityFactor {
						t.Fatalf("expected PriorityFactor to be %v, but got %v", *test.PriorityFactor, queue.PriorityFactor)
					}
					if test.Owners != nil && !reflect.DeepEqual(queue.UserOwners, test.Owners) {
						t.Fatalf("expected UserOwners to be %#v, but got %#v", test.Owners, queue.UserOwners)
					}
					if test.GroupOwners != nil && !reflect.DeepEqual(queue.GroupOwners, test.GroupOwners) {
						t.Fatalf("expected GroupOwners to be %#v, but got %#v", test.GroupOwners, queue.GroupOwners)
					}
					if test.ResourceLimits != nil && !reflect.DeepEqual(queue.ResourceLimits, test.ResourceLimits) {
						t.Fatalf("expected ResourceLimits to be %#v, but got %#v", test.ResourceLimits, queue.ResourceLimits)
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
		"valid priority":        {[]flag{{"priorityFactor", "0.1"}}, makeFloat64Pointer(0.1), nil, nil, nil, nil},
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
				a.Params.QueueAPI.Update = func(queue api.Queue) error {
					a.Out = io.Discard

					// Check that the arguments passed into the API are equal to those provided via CLI flags
					if queue.Name != "arbitrary" {
						t.Fatalf("expected Name to be 'arbitrary', but got %s", queue.Name)
					}
					if test.PriorityFactor != nil && queue.PriorityFactor != *test.PriorityFactor {
						t.Fatalf("expected PriorityFactor to be %v, but got %v", *test.PriorityFactor, queue.PriorityFactor)
					}
					if test.Owners != nil && !reflect.DeepEqual(queue.UserOwners, test.Owners) {
						t.Fatalf("expected UserOwners to be %#v, but got %#v", test.Owners, queue.UserOwners)
					}
					if test.GroupOwners != nil && !reflect.DeepEqual(queue.GroupOwners, test.GroupOwners) {
						t.Fatalf("expected GroupOwners to be %#v, but got %#v", test.GroupOwners, queue.GroupOwners)
					}
					if test.ResourceLimits != nil && !reflect.DeepEqual(queue.ResourceLimits, test.ResourceLimits) {
						t.Fatalf("expected ResourceLimits to be %#v, but got %#v", test.ResourceLimits, queue.ResourceLimits)
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
