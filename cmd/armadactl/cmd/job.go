package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/armadaproject/armada/internal/armadactl"
	"github.com/armadaproject/armada/pkg/client/job"
)

func jobGetCmd() *cobra.Command {
	a := armadactl.New()
	return withOutputFlag(&cobra.Command{
		Use:   "job <job-id> [<job-id> ...]",
		Short: "Get information about one or more jobs",
		Long: "Get the full details of one or more jobs, given by their ids: their state, resources and runs, and the job spec. " +
			"The jobs are read from Lookout, so the lookoutUrl of the context (or --lookoutUrl) must be set. " +
			"The details are printed as yaml, or as json with --output json: " +
			"a single document for a single job, a document with a list of jobs for several.",
		Args: cobra.MinimumNArgs(1),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.GetJobs(args)
		},
	}, a)
}

func jobsGetCmd() *cobra.Command {
	a := armadactl.New()
	cmd := &cobra.Command{
		Use:   "jobs",
		Short: "List jobs",
		Long: "List jobs by querying Lookout, whose request is built with the flags of this command. The lookoutUrl " +
			"of the context (or --lookoutUrl) must be set. The details of each job are printed as yaml, or as json with " +
			"--output json, without the job specs: use get job for those.\n\n" +
			"--filters is a JSON list of Lookout filters, and a job must satisfy all of them. A filter has a field, a match, " +
			"a value and, for annotations, isAnnotation set to true with the annotation's name as the field:\n" +
			`  {"field": "queue", "match": "exact", "value": "my-queue"}` + "\n" +
			"match is one of exact, anyOf, startsWith, contains, greaterThan, lessThan, greaterThanOrEqualTo, " +
			"lessThanOrEqualTo and exists. The value of anyOf is a list. Which matches a field supports is up to Lookout, " +
			"which says so when a filter is not valid. States are in upper case, for example RUNNING.\n\n" +
			"--order is a JSON object with the field to order by and a direction, ASC or DESC.\n\n" +
			"For --filters and --order, a value starting with @ is the path of a file to read the JSON from.",
		Example: `  # Running or queued jobs in a queue
  armadactl get jobs --filters '[
    {"field": "queue", "match": "exact", "value": "my-queue"},
    {"field": "state", "match": "anyOf", "value": ["RUNNING", "QUEUED"]}]'

  # Jobs with an annotation, oldest first, ten at a time
  armadactl get jobs --filters '[{"field": "team", "match": "exact", "value": "data", "isAnnotation": true}]' \
    --order '{"field": "submitted", "direction": "ASC"}' --take 10

  # Filters from a file, as yaml
  armadactl get jobs --filters @filters.json -o yaml`,
		Args: cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			listArgs, err := jobListArgs(cmd)
			if err != nil {
				return err
			}
			return a.ListJobs(listArgs)
		},
	}
	cmd.Flags().String("filters", "[]", "Lookout filters, as a JSON list, or @file to read it from a file.")
	cmd.Flags().String("order", `{"field": "submitted", "direction": "DESC"}`, "How to order the jobs, as a JSON object, or @file to read it from a file.")
	cmd.Flags().Bool("active-job-sets", false, "Only jobs in job sets that are still active.")
	cmd.Flags().Int("skip", 0, "How many jobs to skip, for paging.")
	cmd.Flags().Int("take", 100, "The most jobs to return.")
	return withOutputFlag(cmd, a)
}

// jobListArgs builds the Lookout request from the flags of the get jobs command.
func jobListArgs(cmd *cobra.Command) (job.ListArgs, error) {
	var args job.ListArgs
	f := cmd.Flags()

	filters, err := jsonFlag(cmd, "filters")
	if err != nil {
		return args, err
	}
	if err := decodeStrict(filters, &args.Filters); err != nil {
		return args, fmt.Errorf("invalid --filters: %s", err)
	}
	for i, filter := range args.Filters {
		if filter.Field == "" || filter.Match == "" {
			return args, fmt.Errorf("invalid --filters: filter %d needs a field and a match", i+1)
		}
	}

	order, err := jsonFlag(cmd, "order")
	if err != nil {
		return args, err
	}
	if err := decodeStrict(order, &args.Order); err != nil {
		return args, fmt.Errorf("invalid --order: %s", err)
	}
	if args.Order.Field == "" || args.Order.Direction == "" {
		return args, fmt.Errorf("invalid --order: needs a field and a direction")
	}

	if args.ActiveJobSets, err = f.GetBool("active-job-sets"); err != nil {
		return args, err
	}
	if args.Skip, err = f.GetInt("skip"); err != nil {
		return args, err
	}
	if args.Skip < 0 {
		return args, fmt.Errorf("invalid skip %d: must not be negative", args.Skip)
	}
	if args.Take, err = f.GetInt("take"); err != nil {
		return args, err
	}
	if args.Take < 1 {
		return args, fmt.Errorf("invalid take %d: must be at least 1", args.Take)
	}
	return args, nil
}

// jsonFlag returns the JSON given as the value of a flag, which is the contents of a file when it starts with @.
func jsonFlag(cmd *cobra.Command, name string) ([]byte, error) {
	value, err := cmd.Flags().GetString(name)
	if err != nil {
		return nil, err
	}
	if path, ok := strings.CutPrefix(value, "@"); ok {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("invalid --%s: %s", name, err)
		}
		return b, nil
	}
	return []byte(value), nil
}

// decodeStrict decodes JSON into v, failing on fields v does not have, which would be mistyped names.
func decodeStrict(data []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return err
	}
	if d.More() {
		return fmt.Errorf("unexpected data after the JSON value")
	}
	return nil
}
