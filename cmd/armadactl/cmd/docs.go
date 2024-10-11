package cmd

import (
	"embed"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"runtime"

	"golang.org/x/term"

	"github.com/charmbracelet/glamour"
	"github.com/spf13/cobra"
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

			if term.IsTerminal(int(os.Stdout.Fd())) && runtime.GOOS != "windows" {
				if err = openTextViewer(out); err != nil {
					fmt.Printf("%s\n", out)
					return fmt.Errorf("could not open documentation in text viewer: %s", err)
				}
			} else {
				fmt.Printf("%s\n", out)
			}

			return nil
		},
	}

	return cmd
}

func openTextViewer(text string) error {
	var pagerCmd *exec.Cmd

	tmpFile, err := os.CreateTemp("", "armadactl-docs-*.md")
	if err != nil {
		return fmt.Errorf("could not create temp file for text viewer: %s", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write([]byte(text)); err != nil {
		tmpFile.Close()
		return fmt.Errorf("could not write temp file for text viewer: %s", err)
	}
	tmpFile.Close()

	pagerCmd = exec.Command("less", "-R", tmpFile.Name()) // -R allows ANSI color codes

	pagerCmd.Stdin = os.Stdin
	pagerCmd.Stdout = os.Stdout
	pagerCmd.Stderr = os.Stderr

	return pagerCmd.Run()
}
