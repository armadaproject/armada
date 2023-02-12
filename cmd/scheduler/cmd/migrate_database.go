package cmd

import (
	"context"
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
	cmd.Flags().Duration(
		"timeout",
		5*time.Minute,
		"Duration after which the migration will fail if it has not been created")

	return cmd
}

func migrateDatabase(cmd *cobra.Command, _ []string) error {
	timeout, err := cmd.Flags().GetDuration("timeout")
	if err != nil {
		return errors.WithStack(err)
	}

	config, err := loadConfig()
	if err != nil {
		return err
	}

	log.Info("Beginning scheduler database migration")
	db, err := database.OpenPgxConn(config.Postgres)
	if err != nil {
		return errors.WithMessagef(err, "Failed to connect to database")
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return schedulerdb.Migrate(ctx, db)
}
