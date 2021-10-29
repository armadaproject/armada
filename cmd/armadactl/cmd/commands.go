package cmd

import (
	"github.com/G-Research/armada/cmd/armadactl/cmd/queue"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(
		Create(),
		Delete(),
		Update(),
		Info(),
	)
}

func Create() *cobra.Command {
	command := cobra.Command{
		Use:   "create",
		Short: "Create Armada resource. Supported: queue",
	}

	command.AddCommand(
		queue.Create(),
	)

	return &command
}

func Delete() *cobra.Command {
	command := cobra.Command{
		Use:   "delete",
		Short: "Delete Armada resource. Supported: queue",
	}

	command.AddCommand(
		queue.Delete(),
	)

	return &command
}

func Update() *cobra.Command {
	command := cobra.Command{
		Use:   "update",
		Short: "Update Armada resource. Supported: queue",
	}

	command.AddCommand(
		queue.Update(),
	)

	return &command
}

func Info() *cobra.Command {
	command := cobra.Command{
		Use:   "describe",
		Short: "Retrieve information about armada resource. Supported: queue",
	}

	command.AddCommand(
		queue.Describe(),
	)

	return &command
}
