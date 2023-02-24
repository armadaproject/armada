package repository

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type input struct {
	raw       string
	valuesMap map[string]interface{}
}

type output struct {
	query string
	args  []interface{}
}

type testCase struct {
	name string
	in   *input
	out  *output
}

func TestTemplateSql(t *testing.T) {
	testCases := []*testCase{
		{
			name: "no template strings",
			in: &input{
				raw:       "SELECT * FROM job WHERE job_id = 1",
				valuesMap: map[string]interface{}{},
			},
			out: &output{
				query: "SELECT * FROM job WHERE job_id = 1",
			},
		},
		{
			name: "one template string",
			in: &input{
				raw: "SELECT * FROM job WHERE job_id = <<<id-1>>>",
				valuesMap: map[string]interface{}{
					"id-1": "my-value",
				},
			},
			out: &output{
				query: "SELECT * FROM job WHERE job_id = $1",
				args:  []interface{}{"my-value"},
			},
		},
		{
			name: "failed to parse",
			in: &input{
				raw:       "SELECT * FROM job WHERE job_id = <<<id-1>>",
				valuesMap: map[string]interface{}{},
			},
			out: &output{
				query: "SELECT * FROM job WHERE job_id = <<<id-1>>",
			},
		},
		{
			name: "id not found",
			in: &input{
				raw:       "SELECT * FROM job WHERE job_id = <<<id-1>>>",
				valuesMap: map[string]interface{}{},
			},
			out: &output{
				query: "SELECT * FROM job WHERE job_id = <<<id-1>>>",
			},
		},
		{
			name: "multiple template strings",
			in: &input{
				raw: "SELECT * FROM job WHERE job_id = <<<id-1>>> AND jobset = <<<id-2>>> AND queue = <<<id-3>>>",
				valuesMap: map[string]interface{}{
					"id-1": "my-value",
					"id-2": "other-value",
					"id-3": "hello world",
				},
			},
			out: &output{
				query: "SELECT * FROM job WHERE job_id = $1 AND jobset = $2 AND queue = $3",
				args:  []interface{}{"my-value", "other-value", "hello world"},
			},
		},
		{
			name: "reuse same id",
			in: &input{
				raw: "SELECT * FROM job WHERE job_id = <<<id-1>>> AND jobset = <<<id-2>>> AND queue = <<<id-3>>> AND owner = <<<id-2>>>",
				valuesMap: map[string]interface{}{
					"id-1": "my-value",
					"id-2": "other-value",
					"id-3": "hello world",
				},
			},
			out: &output{
				query: "SELECT * FROM job WHERE job_id = $1 AND jobset = $2 AND queue = $3 AND owner = $2",
				args:  []interface{}{"my-value", "other-value", "hello world"},
			},
		},
		{
			name: "big example",
			in: &input{
				raw: `
					SELECT * FROM job AS j
					LEFT JOIN job_run AS jr ON j.job_id = jr.job_id
					LEFT JOIN user_annotation_lookup AS ual ON j.job_id = ual.job_id
					WHERE
						j.job_id = <<<id-1>>> AND
						j.jobset = <<<id-2>>> AND
						j.queue = <<<id-3>>> AND
						j.owner = <<<id-2>>> AND
						jr.node = <<<node>>> AND
						jr.run_id = <<<runId>>> AND
						ual.key = <<<annotationKey>>>
					ORDER BY
					    j.submission_time ASC`,
				valuesMap: map[string]interface{}{
					"id-1":          "my-value",
					"id-2":          "other-value",
					"id-3":          "hello world",
					"node":          "some-node",
					"runId":         "some-uuid",
					"annotationKey": "key",
				},
			},
			out: &output{
				query: `
					SELECT * FROM job AS j
					LEFT JOIN job_run AS jr ON j.job_id = jr.job_id
					LEFT JOIN user_annotation_lookup AS ual ON j.job_id = ual.job_id
					WHERE
						j.job_id = $1 AND
						j.jobset = $2 AND
						j.queue = $3 AND
						j.owner = $2 AND
						jr.node = $4 AND
						jr.run_id = $5 AND
						ual.key = $6
					ORDER BY
					    j.submission_time ASC`,
				args: []interface{}{"my-value", "other-value", "hello world", "some-node", "some-uuid", "key"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actualSql, actualArgs := templateSql(tc.in.raw, tc.in.valuesMap)
			assert.Equal(t, tc.out.query, actualSql)
			assert.Equal(t, tc.out.args, actualArgs)
		})
	}
}
