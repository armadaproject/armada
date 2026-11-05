package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/pkg/client/job"
)

func TestJobCommands_HaveOutputFlag(t *testing.T) {
	for name, cmd := range map[string]*cobra.Command{"get job": jobGetCmd(), "get jobs": jobsGetCmd()} {
		f := cmd.Flags().Lookup("output")
		require.NotNil(t, f, name)
		assert.Equal(t, "o", f.Shorthand, name)
	}
}

func TestGetJobCmd_NeedsAtLeastOneJobId(t *testing.T) {
	cmd := jobGetCmd()

	assert.Error(t, cmd.Args(cmd, nil))
	assert.NoError(t, cmd.Args(cmd, []string{"a"}))
	assert.NoError(t, cmd.Args(cmd, []string{"a", "b"}))
}

func TestGetJobsCmd_TakesNoArguments(t *testing.T) {
	cmd := jobsGetCmd()

	assert.NoError(t, cmd.Args(cmd, nil))
	assert.Error(t, cmd.Args(cmd, []string{"a"}))
}

func listArgsFor(t *testing.T, flags map[string]string) (job.ListArgs, error) {
	t.Helper()
	cmd := jobsGetCmd()
	for name, value := range flags {
		require.NoError(t, cmd.Flags().Set(name, value), name)
	}
	return jobListArgs(cmd)
}

func writeFile(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "flag.json")
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))
	return path
}

func TestJobListArgs_Defaults(t *testing.T) {
	args, err := listArgsFor(t, nil)

	require.NoError(t, err)
	assert.Empty(t, args.Filters)
	assert.Equal(t, job.Order{Field: "submitted", Direction: "DESC"}, args.Order)
	assert.False(t, args.ActiveJobSets)
	assert.Equal(t, 0, args.Skip)
	assert.Equal(t, 100, args.Take)
}

func TestJobListArgs_ReadsTheFlags(t *testing.T) {
	args, err := listArgsFor(t, map[string]string{
		"filters": `[
			{"field": "queue", "match": "anyOf", "value": ["a", "b"]},
			{"field": "team", "match": "exact", "value": "data", "isAnnotation": true},
			{"field": "team", "match": "exists", "isAnnotation": true}]`,
		"order":           `{"field": "queue", "direction": "ASC"}`,
		"active-job-sets": "true",
		"skip":            "5",
		"take":            "20",
	})

	require.NoError(t, err)
	assert.Equal(t, []job.Filter{
		{Field: "queue", Match: "anyOf", Value: []any{"a", "b"}},
		{Field: "team", Match: "exact", Value: "data", IsAnnotation: true},
		{Field: "team", Match: "exists", IsAnnotation: true},
	}, args.Filters)
	assert.Equal(t, job.Order{Field: "queue", Direction: "ASC"}, args.Order)
	assert.True(t, args.ActiveJobSets)
	assert.Equal(t, 5, args.Skip)
	assert.Equal(t, 20, args.Take)
}

func TestJobListArgs_ReadsFilesWhenTheValueStartsWithAtSign(t *testing.T) {
	args, err := listArgsFor(t, map[string]string{
		"filters": "@" + writeFile(t, `[{"field": "queue", "match": "exact", "value": "a"}]`),
		"order":   "@" + writeFile(t, `{"field": "jobId", "direction": "DESC"}`),
	})

	require.NoError(t, err)
	assert.Equal(t, []job.Filter{{Field: "queue", Match: "exact", Value: "a"}}, args.Filters)
	assert.Equal(t, job.Order{Field: "jobId", Direction: "DESC"}, args.Order)
}

func TestJobListArgs_Rejects(t *testing.T) {
	tests := map[string]struct {
		flags   map[string]string
		wantErr string
	}{
		"filters that are not json":               {map[string]string{"filters": "not-json"}, "invalid --filters"},
		"filters that are not a list":             {map[string]string{"filters": `{"field": "queue"}`}, "invalid --filters"},
		"filters with data after the list":        {map[string]string{"filters": `[] []`}, "unexpected data"},
		"a filter with a mistyped key":            {map[string]string{"filters": `[{"feild": "queue", "match": "exact"}]`}, `unknown field "feild"`},
		"a filter without a field":                {map[string]string{"filters": `[{"match": "exact", "value": "a"}]`}, "filter 1 needs a field and a match"},
		"a filter without a match":                {map[string]string{"filters": `[{"field": "queue", "value": "a"}]`}, "filter 1 needs a field and a match"},
		"the second filter lacking a match":       {map[string]string{"filters": `[{"field": "a", "match": "exact"}, {"field": "b"}]`}, "filter 2 needs"},
		"filters from a file that is not there":   {map[string]string{"filters": "@/no/such/file.json"}, "invalid --filters"},
		"an order that is not json":               {map[string]string{"order": "queue"}, "invalid --order"},
		"an order with a mistyped key":            {map[string]string{"order": `{"feild": "queue", "direction": "ASC"}`}, `unknown field "feild"`},
		"an order without a direction":            {map[string]string{"order": `{"field": "queue"}`}, "needs a field and a direction"},
		"an order without a field":                {map[string]string{"order": `{"direction": "ASC"}`}, "needs a field and a direction"},
		"a take of zero":                          {map[string]string{"take": "0"}, "invalid take"},
		"a negative skip":                         {map[string]string{"skip": "-1"}, "invalid skip"},
		"an order from a file that is not there":  {map[string]string{"order": "@/no/such/file.json"}, "invalid --order"},
		"an order from a file that is not json":   {map[string]string{"order": "@" + writeFile(t, "{")}, "invalid --order"},
		"filters from a file that are not a list": {map[string]string{"filters": "@" + writeFile(t, "{}")}, "invalid --filters"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := listArgsFor(t, tc.flags)

			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}
