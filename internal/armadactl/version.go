package armadactl

import (
	"fmt"
	"text/tabwriter"

	"github.com/armadaproject/armada/internal/armadactl/build"
)

// Version prints build information (e.g., current git commit) to the app output.
func (a *App) Version() error {
	w := tabwriter.NewWriter(a.Out, 1, 1, 1, ' ', 0)
	fmt.Fprintf(w, "Version:\t%s\n", build.ReleaseVersion)
	fmt.Fprintf(w, "Commit:\t%s\n", build.GitCommit)
	fmt.Fprintf(w, "Go version:\t%s\n", build.GoVersion)
	fmt.Fprintf(w, "Built:\t%s\n", build.BuildTime)
	w.Flush()
	return nil
}
