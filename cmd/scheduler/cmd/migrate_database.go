package cmd

import (
	"context"
	"github.com/pkg/errors"
	"time"

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
	return cmd
}

func migrateDatabase(_ *cobra.Command, _ []string) error {
	config, err := loadConfig()
	if err != nil {
		return err
	}
	start := time.Now()
	log.Info("Beginning scheduler database migration")
	db, err := database.OpenPgxConn(config.Postgres)
	if err != nil {
		return errors.Wrapf(err, "Failed to connect to database")
	}
	err = schedulerdb.Migrate(context.Background(), db)
	if err != nil {
		return errors.Wrapf(err, "Failed to migrate scheduler database")
	}
	taken := time.Now().Sub(start)
	log.Infof("Scheduler database migrated in %s", taken)
	return nil
}
