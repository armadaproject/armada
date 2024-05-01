package cmd

import (
	"embed"
	"fmt"
	"github.com/charmbracelet/glamour"
	"github.com/spf13/cobra"
	"io/fs"
)

//go:embed resources/README.md
var ArmadactlReadMe embed.FS

//go:embed resources/glamourStyle.json
var GlamourStyle embed.FS

func docsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "docs",
		Short: "Prints comprehensive Armadactl documentation",
		RunE: func(cmd *cobra.Command, args []string) error {
			readmeContent, err := fs.ReadFile(ArmadactlReadMe, "resources/README.md")
			if err != nil {
				return fmt.Errorf("could not read content from documentation file: %s", err)
			}

			glamourStyleContent, err := fs.ReadFile(GlamourStyle, "resources/glamourStyle.json")
			if err != nil {
				return fmt.Errorf("could not read content from documentation file: %s", err)
			}

			renderer, err := glamour.NewTermRenderer(
				glamour.WithStylesFromJSONBytes(glamourStyleContent),
				glamour.WithWordWrap(120),
			)
			if err != nil {
				return fmt.Errorf("could not create documentation renderer: %s", err)
			}

			out, err := renderer.Render(string(readmeContent))
			if err != nil {
				return fmt.Errorf("could not render content from documentation file: %s", err)
			}

			fmt.Println(out)
			return nil
		},
	}

	return cmd
}
