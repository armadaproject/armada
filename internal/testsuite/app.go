package testsuite

import (
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"text/tabwriter"

	"github.com/G-Research/armada/internal/testsuite/build"
	"github.com/G-Research/armada/pkg/client"
)

type App struct {
	// Parameters passed to the CLI by the user.
	Params *Params
	// Out is used to write the output. Defaults to standard out,
	// but can be overridden in tests to make assertions on the applications's output.
	Out io.Writer
	// Source of randomness. Tests can use a mocked random source in order to provide
	// deterministic testing behavior.
	Random io.Reader
}

// Params struct holds all user-customizable parameters.
// Using a single struct for all CLI commands ensures that all flags are distinct
// and that they can be provided either dynamically on a command line, or
// statically in a config file that's reused between command runs.
type Params struct {
	ApiConnectionDetails *client.ApiConnectionDetails
}

// New instantiates an App with default parameters, including standard output
// and cryptographically secure random source.
func New() *App {
	return &App{
		Params: &Params{},
		Out:    os.Stdout,
		Random: rand.Reader,
	}
}

// validateParams validates a.Params. Currently, it doesn't check anything.
func (a *App) validateParams() error {
	return nil
}

// Version prints build information (e.g., current git commit) to the app output.
func (a *App) Version() error {
	w := tabwriter.NewWriter(a.Out, 1, 1, 1, ' ', 0)
	defer w.Flush()
	fmt.Fprintf(w, "Version:\t%s\n", build.ReleaseVersion)
	fmt.Fprintf(w, "Commit:\t%s\n", build.GitCommit)
	fmt.Fprintf(w, "Go version:\t%s\n", build.GoVersion)
	fmt.Fprintf(w, "Built:\t%s\n", build.BuildTime)
	return nil
}
