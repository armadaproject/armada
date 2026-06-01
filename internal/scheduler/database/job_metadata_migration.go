package database

import "github.com/pkg/errors"

// JobMetadataMigrationPhase is a temporary flag controlling the lazy migration
// of submit_message and groups from the jobs table to a separate job_metadata
// table. It is set in both the scheduler and scheduleringester configs
// controlling read and write behavior, respectively. An unrecognized or empty
// value is rejected.
type JobMetadataMigrationPhase string

const (
	// JobMetadataMigrationPhaseLegacy: reads come from jobs only.
	// Pre-migration baseline. Safe to hold while scheduleringester writes to jobs.
	JobMetadataMigrationPhaseLegacy JobMetadataMigrationPhase = "legacy"
	// JobMetadataMigrationPhaseDualWrite: reads use a LEFT JOIN + COALESCE
	// so pre- and post-migration row shapes both resolve.
	JobMetadataMigrationPhaseDualWrite JobMetadataMigrationPhase = "dualWrite"
	// JobMetadataMigrationPhaseCutover: reads use an INNER JOIN job_metadata.
	// Migration end state. Operators must verify job_metadata has been backfilled
	// for every non-terminal jobs row before selecting this phase
	// (e.g. via: SELECT COUNT(*) FROM jobs j LEFT JOIN job_metadata jm ON j.job_id=jm.job_id
	// WHERE jm.job_id IS NULL AND j.terminated = false);
	JobMetadataMigrationPhaseCutover JobMetadataMigrationPhase = "cutover"
)

func (p JobMetadataMigrationPhase) Validate() error {
	switch p {
	case JobMetadataMigrationPhaseLegacy, JobMetadataMigrationPhaseDualWrite, JobMetadataMigrationPhaseCutover:
		return nil
	}
	return errors.Errorf("invalid JobMetadataMigrationPhase %q: must be one of %q, %q, %q",
		p, JobMetadataMigrationPhaseLegacy, JobMetadataMigrationPhaseDualWrite, JobMetadataMigrationPhaseCutover)
}

// WritesJobs reports whether the scheduleringester should populate the legacy
// jobs.submit_message / jobs.groups columns on new submissions.
func (p JobMetadataMigrationPhase) WritesJobs() bool {
	return p == JobMetadataMigrationPhaseLegacy || p == JobMetadataMigrationPhaseDualWrite
}

// WritesJobMetadata reports whether the scheduleringester should insert a
// job_metadata row alongside each new jobs row.
func (p JobMetadataMigrationPhase) WritesJobMetadata() bool {
	return p == JobMetadataMigrationPhaseDualWrite || p == JobMetadataMigrationPhaseCutover
}
