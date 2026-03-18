package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTargetsJobTable(t *testing.T) {
	tests := []struct {
		name     string
		stmt     string
		expected bool
	}{
		{
			name:     "standard lowercase",
			stmt:     "ALTER TABLE job SET (autovacuum_vacuum_scale_factor = 0.01)",
			expected: true,
		},
		{
			name:     "mixed case table name",
			stmt:     "ALTER TABLE JOB SET (autovacuum_vacuum_scale_factor = 0.01)",
			expected: true,
		},
		{
			name:     "job_run not matched",
			stmt:     "ALTER TABLE job_run SET (autovacuum_vacuum_scale_factor = 0.01)",
			expected: false,
		},
		{
			name:     "comment containing job before alter table",
			stmt:     "-- tune the job table\nALTER TABLE job SET (autovacuum_vacuum_scale_factor = 0.01)",
			expected: true,
		},
		{
			name:     "comment containing job but targets job_run",
			stmt:     "-- tune the job table\nALTER TABLE job_run SET (autovacuum_vacuum_scale_factor = 0.01)",
			expected: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, targetsJobTable(tc.stmt))
		})
	}
}

func TestReplaceJobTable(t *testing.T) {
	tests := []struct {
		name      string
		stmt      string
		partition string
		expected  string
	}{
		{
			name:      "standard lowercase",
			stmt:      "ALTER TABLE job SET (autovacuum_vacuum_scale_factor = 0.01)",
			partition: "job_p2024",
			expected:  "ALTER TABLE job_p2024 SET (autovacuum_vacuum_scale_factor = 0.01)",
		},
		{
			name:      "mixed case table name",
			stmt:      "ALTER TABLE JOB SET (autovacuum_vacuum_scale_factor = 0.01)",
			partition: "job_p2024",
			expected:  "ALTER TABLE job_p2024 SET (autovacuum_vacuum_scale_factor = 0.01)",
		},
		{
			name:      "comment containing job before alter table",
			stmt:      "-- tune the job table\nALTER TABLE job SET (autovacuum_vacuum_scale_factor = 0.01)",
			partition: "job_p2024",
			expected:  "-- tune the job table\nALTER TABLE job_p2024 SET (autovacuum_vacuum_scale_factor = 0.01)",
		},
		{
			name:      "reset statement",
			stmt:      "ALTER TABLE job RESET (autovacuum_vacuum_scale_factor)",
			partition: "job_p2024",
			expected:  "ALTER TABLE job_p2024 RESET (autovacuum_vacuum_scale_factor)",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, replaceJobTable(tc.stmt, tc.partition))
		})
	}
}
