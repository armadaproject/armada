package cmd

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/G-Research/armada/cmd/armadactl/build"
)

func init() {
	rootCmd.AddCommand(versionCmd)
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print client version information",
	Run: func(cmd *cobra.Command, args []string) {
		w := tabwriter.NewWriter(os.Stdout, 1, 1, 1, ' ', 0)
		fmt.Fprintf(w, "Version:\t%s\n", build.ReleaseVersion)
		fmt.Fprintf(w, "Commit:\t%s\n", build.GitCommit)
		fmt.Fprintf(w, "Go version:\t%s\n", build.GoVersion)
		fmt.Fprintf(w, "Built:\t%s\n", build.BuildTime)
		w.Flush()
	},
}
