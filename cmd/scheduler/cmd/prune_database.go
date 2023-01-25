package cmd

import (
	"context"
	"k8s.io/apimachinery/pkg/util/clock"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/armadaproject/armada/internal/common/database"
	schedulerdb "github.com/armadaproject/armada/internal/scheduler/database"
)

func pruneDbCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pruneDatabase",
		Short: "removes old data from the database",
		RunE:  migrateDatabase,
	}
	cmd.PersistentFlags().Duration(
		"timeout",
		5*time.Minute,
		"Duration after which the job will fail if it has not completed")
	cmd.PersistentFlags().Int(
		"batchsize",
		10000,
		"Number of rows that will be deleted in a single batch")
	cmd.PersistentFlags().Duration(
		"expireAfter",
		2*time.Hour,
		"Length of time after completion that job data will be removed")
	return cmd
}

func pruneDatabase(_ *cobra.Command, _ []string) error {
	timeout := viper.GetDuration("timeout")
	batchSize := viper.GetInt("batchsize")
	expireAfter := viper.GetDuration("expireAfter")
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	config, err := loadConfig()
	if err != nil {
		return err
	}
	db, err := database.OpenPgxConn(config.Postgres)
	if err != nil {
		return errors.WithMessagef(err, "Failed to connect to database")
	}
	return schedulerdb.PruneDb(ctx, db, batchSize, expireAfter, clock.RealClock{})
}
