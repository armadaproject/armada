package repository

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFromBuilder_SingleTable(t *testing.T) {
	out := NewFromBuilder("job", "j").
		Build()
	assert.Equal(t, splitByWhitespace("FROM job AS j"), splitByWhitespace(out))
}

func TestFromBuilder_ManyTables(t *testing.T) {
	out := NewFromBuilder("job", "j").
		Join(Left, "job_run", "jr", "job_id").
		Join(Inner, "( SELECT * FROM user_annotation_lookup WHERE key = <something> AND value = <something> )", "ct", "job_id").
		Join(Inner, "other_table", "ot", "other_column").
		Build()
	assert.Equal(t, splitByWhitespace(`
		FROM job AS j
		LEFT JOIN job_run AS jr ON j.job_id = jr.job_id
		INNER JOIN (
			SELECT * FROM user_annotation_lookup
			WHERE key = <something> AND value = <something>
		) AS ct ON j.job_id = ct.job_id
		INNER JOIN other_table AS ot ON j.other_column = ot.other_column
	`), splitByWhitespace(out))
}
