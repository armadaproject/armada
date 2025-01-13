package cmd

import (
	"fmt"
	"os"
	"runtime/pprof"
	"time"

	"github.com/spf13/cobra"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/scheduler/simulator"
	"github.com/armadaproject/armada/internal/scheduler/simulator/sink"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

func RootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "Simulate",
		Short: "Simulate running jobs on Armada.",
		RunE:  runSimulations,
	}
	cmd.Flags().String("clusters", "", "Path specifying cluster configurations to simulate.")
	cmd.Flags().String("workloads", "", "Path specifying workloads to simulate.")
	cmd.Flags().String("config", "", "Path to scheduler configurations to simulate. Uses a default config if not provided.")
	cmd.Flags().Bool("showSchedulerLogs", false, "Show scheduler logs.")
	cmd.Flags().String("outputDir", "", "Path to directory where output files will be written.  Defaults to timestamped directory.")
	cmd.Flags().Bool("overwriteOutputDir", false, "Overwrite output director if it already exists.  If false then an error will be thrown if the directory already exists")
	cmd.Flags().Bool("enableFastForward", false, "Skips schedule events when we're in a steady state")
	cmd.Flags().Int("hardTerminationMinutes", -1, "Limit the time simulated.  -1 for no limit.")
	cmd.Flags().Int("schedulerCyclePeriodSeconds", 10, "How often we should trigger schedule events")
	cmd.Flags().Bool("profile", false, "If true then the simulator will be profiled and a profiling file written to the output directory")
	return cmd
}

func runSimulations(cmd *cobra.Command, args []string) error {
	// Get command-line arguments.
	clusterFile, err := cmd.Flags().GetString("clusters")
	if err != nil {
		return err
	}
	workloadFile, err := cmd.Flags().GetString("workloads")
	if err != nil {
		return err
	}
	configFile, err := cmd.Flags().GetString("config")
	if err != nil {
		return err
	}
	showSchedulerLogs, err := cmd.Flags().GetBool("showSchedulerLogs")
	if err != nil {
		return err
	}
	outputDirPath, err := cmd.Flags().GetString("outputDir")
	if err != nil {
		return err
	}
	overwriteDirIfExists, err := cmd.Flags().GetBool("overwriteOutputDir")
	if err != nil {
		return err
	}

	enableFastForward, err := cmd.Flags().GetBool("enableFastForward")
	if err != nil {
		return err
	}
	hardTerminationMinutes, err := cmd.Flags().GetInt("hardTerminationMinutes")
	if err != nil {
		return err
	}
	schedulerCyclePeriodSeconds, err := cmd.Flags().GetInt("schedulerCyclePeriodSeconds")
	if err != nil {
		return err
	}
	shouldProfile, err := cmd.Flags().GetBool("profile")
	if err != nil {
		return err
	}

	if outputDirPath == "" {
		outputDirPath = fmt.Sprintf("armada_simulator_%s", time.Now().Format("2006_01_02_15_04_05"))
	}

	if pathExists(outputDirPath) && overwriteDirIfExists {
		err := os.RemoveAll(outputDirPath)
		if err != nil {
			return err
		}
	} else if pathExists(outputDirPath) {
		return fmt.Errorf("output directory %s already exists and overwriteOutputDir not set", outputDirPath)
	}

	err = os.MkdirAll(outputDirPath, 0o777)
	if err != nil {
		return err
	}

	// Load test specs. and config.
	clusterSpec, err := simulator.ClusterSpecFromFilePath(clusterFile)
	if err != nil {
		return err
	}
	workloadSpec, err := simulator.WorkloadSpecFromFilePath(workloadFile)
	if err != nil {
		return err
	}

	schedulingConfig := testfixtures.TestSchedulingConfig()
	if configFile != "" {
		schedulingConfig, err = simulator.SchedulingConfigFromFilePath(configFile)
		if err != nil {
			return err
		}
	}

	ctx := armadacontext.Background()
	outputSink, err := sink.NewParquetSink(outputDirPath)
	if err != nil {
		return err
	}
	defer outputSink.Close(ctx)

	ctx.Info("Armada simulator")
	ctx.Infof("ClusterSpec: %v", clusterFile)
	ctx.Infof("WorkloadSpecs: %v", workloadFile)
	ctx.Infof("SchedulingConfig: %v", configFile)
	ctx.Infof("OutputDir: %v", outputDirPath)

	s, err := simulator.NewSimulator(
		clusterSpec, workloadSpec, schedulingConfig, enableFastForward, hardTerminationMinutes, schedulerCyclePeriodSeconds, outputSink)
	if err != nil {
		return err
	}

	if shouldProfile {
		profilingFile := outputDirPath + "/profile"
		log.Infof("Will write profiling information to %s", profilingFile)
		f, err := os.Create(profilingFile)
		if err != nil {
			log.Fatal(err)
		}
		err = pprof.StartCPUProfile(f)
		if err != nil {
			log.Fatal(err)
		}
		defer pprof.StopCPUProfile()
	}

	if !showSchedulerLogs {
		s.SuppressSchedulerLogs = true
	} else {
		ctx.Info("Showing scheduler logs")
	}

	g, ctx := armadacontext.ErrGroup(ctx)
	g.Go(func() error {
		return s.Run(ctx)
	})

	// Wait for simulations to complete.
	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

func pathExists(path string) bool {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}
	return err == nil
}
