package oidc

import (
	"fmt"
	"os/exec"
	"runtime"
)

func openBrowser(url string) (*exec.Cmd, error) {
	cmd := browserCommand(url)
	return cmd, cmd.Start()
}

func browserCommand(url string) *exec.Cmd {
	switch runtime.GOOS {
	case "linux":
		return exec.Command("xdg-open", url)
	case "windows":
		return exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	case "darwin":
		return exec.Command("open", url)
	}
	panic(fmt.Errorf("unsupported platform"))
}
