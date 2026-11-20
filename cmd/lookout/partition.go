package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"golang.org/x/term"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/database"
	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/lookout/configuration"
	lookoutschema "github.com/armadaproject/armada/internal/lookout/schema"
)

var executeOptions = lookoutschema.ExecuteOptions{
	LockTimeout:      5 * time.Second,
	LockAttempts:     12,
	ProgressInterval: time.Minute,
}

func partitionJobTable(ctx *armadacontext.Context, config configuration.LookoutConfig) error {
	preflight, printSql, execute := viper.GetBool(Preflight), viper.GetBool(PrintSql), viper.GetBool(Execute)
	switch {
	case !viper.GetBool(PartitionJobTable):
		return errors.Errorf("--%s, --%s, --%s and --%s can only be used with --%s", Preflight, PrintSql, Execute, Yes, PartitionJobTable)
	case countTrue(preflight, printSql, execute) > 1:
		return errors.Errorf("choose at most one of --%s, --%s and --%s", Preflight, PrintSql, Execute)
	case viper.GetBool(Yes) && !execute:
		return errors.Errorf("--%s can only be used with --%s", Yes, Execute)
	}

	db, err := database.OpenPgxConn(config.Postgres)
	if err != nil {
		return err
	}
	defer db.Close(ctx)
	if err := database.PrepareSchema(ctx, db, database.MigrationConfig{Schema: config.Migration.Schema}); err != nil {
		return err
	}

	switch {
	case printSql:
		shape, _, err := lookoutschema.DetectJobTableShape(ctx, db)
		if err != nil {
			return err
		}
		if shape != lookoutschema.JobTableShapeUnpartitioned {
			return errors.Errorf("the job table is %s, so there is no SQL to print", shape)
		}
		if err := lookoutschema.CheckMigrationsApplied(ctx, db); err != nil {
			return err
		}
		script, err := lookoutschema.ConversionScript(ctx, db)
		if err != nil {
			return err
		}
		_, err = fmt.Print(script)
		return err
	case execute:
		if !viper.GetBool(Yes) {
			if err := confirm(); err != nil {
				return err
			}
		}
		start := time.Now()
		converted, err := lookoutschema.ExecutePartitionJobTable(ctx, db, executeOptions)
		if err != nil {
			return errors.WithMessage(err, "partitioning the job table failed; the job table is unchanged")
		}
		if converted {
			log.Infof("Partitioned the job table in %s", time.Since(start).Round(time.Second))
		} else {
			log.Info("The job table is already partitioned. There is nothing to do.")
		}
		return nil
	default:
		report, err := lookoutschema.Preflight(ctx, db)
		if err != nil {
			return err
		}
		if err := report.Write(os.Stdout); err != nil {
			return err
		}
		if len(report.Blockers()) > 0 {
			return errors.New("preflight found problems that would stop the migration")
		}
		return nil
	}
}

func confirm() error {
	if !term.IsTerminal(int(os.Stdin.Fd())) {
		return errors.Errorf("pass --%s to confirm partitioning the job table", Yes)
	}
	fmt.Print("Partitioning holds an exclusive lock on the job table until it finishes, so Lookout and job-status queries\n" +
		"wait and ingestion pauses. Run --preflight first. Type yes to continue: ")
	answer, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil {
		return err
	}
	if strings.TrimSpace(answer) != "yes" {
		return errors.New("not confirmed; the job table is unchanged")
	}
	return nil
}

func countTrue(values ...bool) int {
	n := 0
	for _, v := range values {
		if v {
			n++
		}
	}
	return n
}
