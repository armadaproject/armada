package cmd

import (
	"github.com/spf13/cobra"

	"github.com/armadaproject/armada/internal/armadactl"
	"github.com/armadaproject/armada/pkg/client"
	cq "github.com/armadaproject/armada/pkg/client/queue"
)

// initParams initialises the command parameters, flags, and a configuration file.
func initParams(cmd *cobra.Command, params *armadactl.Params) error {
	// Stuff above this is from the example
	err := client.LoadCommandlineArgs()
	if err != nil {
		return err
	}
	params.ApiConnectionDetails = client.ExtractCommandlineArmadaApiConnectionDetails()

	// Setup the armadactl to use pkg/client as its backend for queue-related commands
	params.QueueAPI.Create = cq.Create(client.ExtractCommandlineArmadaApiConnectionDetails)
	params.QueueAPI.Delete = cq.Delete(client.ExtractCommandlineArmadaApiConnectionDetails)
	params.QueueAPI.GetInfo = cq.GetInfo(client.ExtractCommandlineArmadaApiConnectionDetails)
	params.QueueAPI.Get = cq.Get(client.ExtractCommandlineArmadaApiConnectionDetails)
	params.QueueAPI.Update = cq.Update(client.ExtractCommandlineArmadaApiConnectionDetails)

	return nil
}
