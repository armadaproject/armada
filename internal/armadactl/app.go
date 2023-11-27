/*
Package armadactl contains all the business logic for armadactl.
It has no dependency on either viper or cobra libraries, and can be unit tested.

Output writer for the App is configurable so that tests can easily capture
and perform assertions on it. Params are initialised in the param package,
so that this package can be clear of viper dependency.

# TODO there should be a type that uniquely represents a job, instead of having to pass around several parameters

TODO add methods for querying more detailed info about queues and jobs (current priority and so on)
*/
package armadactl

import (
	"crypto/rand"
	"io"
	"os"

	"github.com/armadaproject/armada/pkg/client"
	"github.com/armadaproject/armada/pkg/client/queue"
)

type App struct {
	// Parameters passed to the CLI by the user.
	Params *Params
	// Out is used to write the output. Default to standard out,
	// but can be overridden in tests to make assertions on the applications's output.
	Out io.Writer
	// Source of randomness. Tests can use a mocked random source in order to provide
	// deterministic testing behaviour.
	Random io.Reader
}

// Params struct holds all user-customizable parameters.
// Using a single struct for all CLI commands ensures that all flags are distinct
// and that they can be provided either dynamically on a command line, or
// statically in a config file that's reused between command runs.
type Params struct {
	ApiConnectionDetails *client.ApiConnectionDetails
	QueueAPI             *QueueAPI
}

// QueueAPI struct holds pointers to functions that are called by armadactl.
// The function types are defined in in /pkg/client/queue.
// By default, they point to functions defined in /pkg/client/queue, which call the Armada server gRPC API.
// However, they are user-replaceable to facilitate testing.
// TODO Consider replacing with an interface
type QueueAPI struct {
	Create  queue.CreateAPI
	Delete  queue.DeleteAPI
	GetInfo queue.GetInfoAPI
	Get     queue.GetAPI
	Update  queue.UpdateAPI
}

// New instantiates an App with default parameters, including standard output
// and cryptographically secure random source.
func New() *App {
	app := &App{
		Params: &Params{},
		Out:    os.Stdout,
		Random: rand.Reader,
	}
	app.Params.QueueAPI = &QueueAPI{}
	return app
}
