package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/armadaproject/armada/internal/armadactl"
)

func execCmd() *cobra.Command {
	a := armadactl.New()
	var (
		runID     string
		container string
		podNumber int32
		tty       bool
		stdin     bool
	)

	cmd := &cobra.Command{
		Use:   "exec <job-id> -- <command...>",
		Short: "Execute a command in a running job's container.",
		Long: `Open an interactive exec session to a running Armada job.

The job must be in the RUNNING state. By default, TTY and stdin are enabled
when the local terminal is a TTY.

Examples:
  # Interactive shell
  armadactl exec abc123 -- /bin/bash

  # Non-interactive command
  armadactl exec abc123 -- ls -la /tmp

  # Specific container
  armadactl exec abc123 --container sidecar -- cat /tmp/foo

  # Specific pod (multi-pod job)
  armadactl exec abc123 --pod 2 -- sh`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 {
				return fmt.Errorf("job ID is required")
			}
			if len(args) < 2 {
				return fmt.Errorf("command is required; use -- to separate it (e.g. exec <job-id> -- /bin/bash)")
			}
			return nil
		},
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			jobID := args[0]
			command := args[1:]

			// Default TTY/stdin to true if local stdin is a terminal,
			// unless the user explicitly set the flags.
			localTTY := term.IsTerminal(int(os.Stdin.Fd()))
			if !cmd.Flags().Changed("tty") {
				tty = localTTY
			}
			if !cmd.Flags().Changed("stdin") {
				stdin = localTTY
			}

			return a.Exec(armadactl.ExecParams{
				JobID:     jobID,
				RunID:     runID,
				Container: container,
				PodNumber: podNumber,
				TTY:       tty,
				Stdin:     stdin,
				Command:   command,
			})
		},
	}

	cmd.Flags().StringVar(&runID, "run-id", "", "Specific run ID (default: latest active run)")
	cmd.Flags().StringVar(&container, "container", "", "Container name (default: first container)")
	cmd.Flags().Int32VarP(&podNumber, "pod", "p", 0, "Pod number for multi-pod jobs (default: 0)")
	cmd.Flags().BoolVarP(&tty, "tty", "t", false, "Allocate a TTY (default: true if stdin is a terminal)")
	cmd.Flags().BoolVarP(&stdin, "stdin", "i", false, "Pass stdin to the container (default: true if stdin is a terminal)")

	return cmd
}
