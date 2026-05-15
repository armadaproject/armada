package database

import "github.com/pkg/errors"

// JobSpecMigrationPhase is a temporary flag controlling the lazy migration of
// submit_message and groups from the jobs table to a separate job_specs table.
// It is set in both the scheduler and scheduleringester configs controlling
// read and write behavior, respectively. An unrecognized or empty value is rejected.
type JobSpecMigrationPhase string

const (
	// JobSpecMigrationPhaseLegacy: reads come from jobs only.
	// Pre-migration baseline. Safe to hold while scheduleringester writes to jobs.
	JobSpecMigrationPhaseLegacy JobSpecMigrationPhase = "legacy"
	// JobSpecMigrationPhaseDualWrite: reads use a LEFT JOIN + COALESCE
	// so pre- and post-migration row shapes both resolve.
	JobSpecMigrationPhaseDualWrite JobSpecMigrationPhase = "dualWrite"
	// JobSpecMigrationPhaseCutover: reads use an INNER JOIN job_specs.
	// Migration end state. Operators must verify job_specs has been backfilled
	// for every non-terminal jobs row before selecting this phase
	// (e.g. via: SELECT COUNT(*) FROM jobs j LEFT JOIN job_specs js ON j.job_id=js.job_id
	// WHERE js.job_id IS NULL AND j.terminated = false);
	JobSpecMigrationPhaseCutover JobSpecMigrationPhase = "cutover"
)

func (p JobSpecMigrationPhase) Validate() error {
	switch p {
	case JobSpecMigrationPhaseLegacy, JobSpecMigrationPhaseDualWrite, JobSpecMigrationPhaseCutover:
		return nil
	}
	return errors.Errorf("invalid JobSpecMigrationPhase %q: must be one of %q, %q, %q",
		p, JobSpecMigrationPhaseLegacy, JobSpecMigrationPhaseDualWrite, JobSpecMigrationPhaseCutover)
}

// WritesJobs reports whether the scheduleringester should populate the legacy
// jobs.submit_message / jobs.groups columns on new submissions.
func (p JobSpecMigrationPhase) WritesJobs() bool {
	return p == JobSpecMigrationPhaseLegacy || p == JobSpecMigrationPhaseDualWrite
}

// WritesJobSpecs reports whether the scheduleringester should insert a
// job_specs row alongside each new jobs row.
func (p JobSpecMigrationPhase) WritesJobSpecs() bool {
	return p == JobSpecMigrationPhaseDualWrite || p == JobSpecMigrationPhaseCutover
}
