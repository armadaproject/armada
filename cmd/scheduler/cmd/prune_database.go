package cmd

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"k8s.io/utils/clock"

	"github.com/armadaproject/armada/internal/common/database"
	schedulerdb "github.com/armadaproject/armada/internal/scheduler/database"
)

func pruneDbCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pruneDatabase",
		Short: "removes old data from the database",
		RunE:  pruneDatabase,
	}
	cmd.Flags().Duration(
		"timeout",
		5*time.Minute,
		"Duration after which the job will fail if it has not completed")
	cmd.Flags().Int(
		"batchsize",
		10000,
		"Number of rows that will be deleted in a single batch")
	cmd.Flags().Duration(
		"expireAfter",
		2*time.Hour,
		"Length of time after completion that job data will be removed")
	return cmd
}

func pruneDatabase(cmd *cobra.Command, _ []string) error {
	timeout, err := cmd.Flags().GetDuration("timeout")
	if err != nil {
		return errors.WithStack(err)
	}
	batchSize, err := cmd.Flags().GetInt("batchsize")
	if err != nil {
		return errors.WithStack(err)
	}
	expireAfter, err := cmd.Flags().GetDuration("expireAfter")
	if err != nil {
		return errors.WithStack(err)
	}

	config, err := loadConfig()
	if err != nil {
		return err
	}

	db, err := database.OpenPgxConn(config.Postgres)
	if err != nil {
		return errors.WithMessagef(err, "Failed to connect to database")
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return schedulerdb.PruneDb(ctx, db, batchSize, expireAfter, clock.RealClock{})
}
