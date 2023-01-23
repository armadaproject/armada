package cmd

import (
	"context"
	"github.com/spf13/viper"
	"time"

	"github.com/pkg/errors"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/armadaproject/armada/internal/common/database"
	schedulerdb "github.com/armadaproject/armada/internal/scheduler/database"
)

func migrateDbCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrateDatabase",
		Short: "migrates the scheduler database to the latest version",
		RunE:  migrateDatabase,
	}
	cmd.PersistentFlags().Duration(
		"timeout",
		5*time.Minute,
		"Duration after which the migration will fail if it has not been created")

	return cmd
}

func migrateDatabase(_ *cobra.Command, _ []string) error {
	timeout := viper.GetDuration("timeout")
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	config, err := loadConfig()
	if err != nil {
		return err
	}
	start := time.Now()
	log.Info("Beginning scheduler database migration")
	db, err := database.OpenPgxConn(config.Postgres)
	if err != nil {
		return errors.WithMessagef(err, "Failed to connect to database")
	}
	err = schedulerdb.Migrate(ctx, db)
	if err != nil {
		return errors.WithMessagef(err, "Failed to migrate scheduler database")
	}
	taken := time.Since(start)
	log.Infof("Scheduler database migrated in %s", taken)
	return nil
}
