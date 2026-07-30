package db

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/armadaproject/armada/internal/common/logging"
)

//go:embed sql/partition_up.sql
var partitionMigrationSQL string

//go:embed sql/partition_down.sql
var partitionRevertSQL string

// applyPartitionMigration applies the partition-by-submitted migration,
// then creates daily partitions covering all configured jobAgeDays plus
// a buffer for live ingestion, plus a DEFAULT partition.
func (p *PostgresDatabase) applyPartitionMigration(ctx context.Context, jobAgeDays []int) error {
	// Check whether the job table is already partitioned (relkind 'p').
	// If so, skip the one-time conversion and just ensure partitions exist.
	var relkind string
	if err := p.pool.QueryRow(ctx,
		"SELECT relkind::text FROM pg_class WHERE relname = 'job'").Scan(&relkind); err != nil {
		return fmt.Errorf("checking job table type: %w", err)
	}

	if relkind != "p" {
		if _, err := p.pool.Exec(ctx, partitionMigrationSQL); err != nil {
			return fmt.Errorf("applying partition migration: %w", err)
		}

		if err := p.createDailyPartitions(ctx, jobAgeDays); err != nil {
			return fmt.Errorf("creating daily partitions: %w", err)
		}

		// Move any existing rows from the old unpartitioned table into the
		// new partitioned one, then drop the old table. This must happen
		// after partitions exist so Postgres can route the rows.
		if _, err := p.pool.Exec(ctx, "INSERT INTO job SELECT * FROM job_unpartitioned"); err != nil {
			return fmt.Errorf("moving rows to partitioned table: %w", err)
		}
		if _, err := p.pool.Exec(ctx, "DROP TABLE job_unpartitioned"); err != nil {
			return fmt.Errorf("dropping old unpartitioned table: %w", err)
		}

		logging.Info("Converted job table to range-partitioned")
	} else {
		// Already partitioned — just ensure any new date partitions exist.
		if err := p.createDailyPartitions(ctx, jobAgeDays); err != nil {
			return fmt.Errorf("creating daily partitions: %w", err)
		}
		logging.Info("Job table already partitioned, ensured partitions are up to date")
	}

	return nil
}

// createDailyPartitions creates one partition per day covering all dates
// in jobAgeDays (relative to today) plus a 2-day forward buffer, and a
// DEFAULT partition as a safety net.
func (p *PostgresDatabase) createDailyPartitions(ctx context.Context, jobAgeDays []int) error {
	today := time.Now().Truncate(24 * time.Hour)

	dates := make(map[time.Time]struct{})
	for _, days := range jobAgeDays {
		date := today.AddDate(0, 0, -days)
		dates[date] = struct{}{}
	}
	// Add today + 2 days buffer for live ingestion
	for offset := 0; offset <= 2; offset++ {
		dates[today.AddDate(0, 0, offset)] = struct{}{}
	}

	for date := range dates {
		partitionName := fmt.Sprintf("job_p%s", date.Format("20060102"))
		nextDay := date.AddDate(0, 0, 1)
		sql := fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s PARTITION OF job FOR VALUES FROM ('%s') TO ('%s')",
			partitionName,
			date.Format("2006-01-02"),
			nextDay.Format("2006-01-02"),
		)
		if _, err := p.pool.Exec(ctx, sql); err != nil {
			return fmt.Errorf("creating partition %s: %w", partitionName, err)
		}
		logging.Infof("Created partition %s", partitionName)
	}

	if _, err := p.pool.Exec(ctx, "CREATE TABLE IF NOT EXISTS job_default PARTITION OF job DEFAULT"); err != nil {
		return fmt.Errorf("creating default partition: %w", err)
	}
	logging.Info("Created default partition job_default")

	return nil
}
