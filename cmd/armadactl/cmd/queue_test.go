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
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/armadactl"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client/queue"
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
	}{
		"default flags":         {nil, nil, nil, nil, nil},
		"valid priority":        {[]flag{{"priorityFactor", "1.0"}}, makeFloat64Pointer(1.0), nil, nil, nil},
		"valid owners":          {[]flag{{"owners", "user1,user2"}}, nil, []string{"user1", "user2"}, nil, nil},
		"valid group owners":    {[]flag{{"groupOwners", "group1,group2"}}, nil, nil, []string{"group1", "group2"}, nil},
		"valid resource limits": {[]flag{{"resourceLimits", "cpu=0.3,memory=0.2"}}, nil, nil, nil, map[string]float64{"cpu": 0.3, "memory": 0.2}},
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
					require.True(t, q.Name == "arbitrary")

					if test.PriorityFactor != nil {
						require.True(t, float64(q.PriorityFactor) == *test.PriorityFactor)
					}
					if test.Owners != nil {
						require.True(t, reflect.DeepEqual(q.Permissions, permissions))
					}

					if test.ResourceLimits != nil {
						for resourceName, resourceLimit := range q.ResourceLimits {
							require.Equal(t, test.ResourceLimits[string(resourceName)], float64(resourceLimit), "resource limit mismatch")
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
				require.NoError(t, cmd.Flags().Set(flag.name, flag.value))
			}

			require.NoError(t, cmd.Execute())
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
			require.True(t, name == "arbitrary")
			return nil
		}
		return nil
	}

	// Arbitrary queue name
	cmd.SetArgs([]string{"arbitrary"})

	require.NoError(t, cmd.Execute())
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
			require.Equal(t, name, "arbitrary")
			return nil, fmt.Errorf("expected test error to force armadactl.DescribeQueue to return")
		}
		return nil
	}

	// Arbitrary queue name
	cmd.SetArgs([]string{"arbitrary"})

	// Execute the command and check any error
	require.ErrorContains(t, cmd.Execute(), "expected test error")
}

func TestUpdate(t *testing.T) {
	// TODO there are no tests for invalid input because cobra silently discards those inputs without raising errors
	tests := map[string]struct {
		Flags          []flag
		PriorityFactor *float64
		Owners         []string
		GroupOwners    []string
		ResourceLimits map[string]float64
	}{
		"default flags":         {nil, nil, nil, nil, nil},
		"valid priority":        {[]flag{{"priorityFactor", "1.0"}}, makeFloat64Pointer(1.0), nil, nil, nil},
		"valid owners":          {[]flag{{"owners", "user1,user2"}}, nil, []string{"user1", "user2"}, nil, nil},
		"valid group owners":    {[]flag{{"groupOwners", "group1,group2"}}, nil, nil, []string{"group1", "group2"}, nil},
		"valid resource limits": {[]flag{{"resourceLimits", "cpu=0.3,memory=0.2"}}, nil, nil, nil, map[string]float64{"cpu": 0.3, "memory": 0.2}},
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
					require.Equal(t, q.Name, "arbitrary")
					if test.PriorityFactor != nil {
						require.True(t, float64(q.PriorityFactor) == *test.PriorityFactor)
					}
					if test.Owners != nil {
						require.True(t, reflect.DeepEqual(q.Permissions, permissions))
					}

					if test.ResourceLimits != nil {
						for resourceName, resourceLimit := range q.ResourceLimits {
							require.Equal(t, test.ResourceLimits[string(resourceName)], float64(resourceLimit), "resource limit mismatch")
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
				require.NoError(t, cmd.Flags().Set(flag.name, flag.value))
			}

			// Execute the command and check any error
			require.NoError(t, cmd.Execute())
		})
	}
}
