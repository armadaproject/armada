package cmd

import (
	"github.com/spf13/cobra"

	"github.com/G-Research/armada/cmd/armadactl/cmd/queue"

	"github.com/G-Research/armada/pkg/client"
	cq "github.com/G-Research/armada/pkg/client/queue"
)

func init() {
	rootCmd.AddCommand(
		Create(client.ExtractCommandlineArmadaApiConnectionDetails),
		Delete(client.ExtractCommandlineArmadaApiConnectionDetails),
		Update(client.ExtractCommandlineArmadaApiConnectionDetails),
		Info(client.ExtractCommandlineArmadaApiConnectionDetails),
	)
}

func Create(connectionDetails client.ConnectionDetails) *cobra.Command {
	command := cobra.Command{
		Use:   "create",
		Short: "Create Armada resource. Supported: queue",
	}

	command.AddCommand(
		queue.Create(cq.Create(connectionDetails)),
	)

	return &command
}

func Delete(connectionDetails client.ConnectionDetails) *cobra.Command {
	command := cobra.Command{
		Use:   "delete",
		Short: "Delete Armada resource. Supported: queue",
	}

	command.AddCommand(
		queue.Delete(cq.Delete(connectionDetails)),
	)

	return &command
}

func Update(connectionDetails client.ConnectionDetails) *cobra.Command {
	command := cobra.Command{
		Use:   "update",
		Short: "Update Armada resource. Supported: queue",
	}

	command.AddCommand(
		queue.Update(cq.Update(connectionDetails)),
	)

	return &command
}

func Info(connectionDetails client.ConnectionDetails) *cobra.Command {
	command := cobra.Command{
		Use:   "describe",
		Short: "Retrieve information about armada resource. Supported: queue",
	}

	command.AddCommand(
		queue.Describe(cq.GetInfo(connectionDetails)),
	)

	return &command
}
