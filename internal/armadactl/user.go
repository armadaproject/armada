package armadactl

import (
	"fmt"
	"strings"
)

func (a *App) actionUser() string {
	if a == nil || a.Params == nil || a.Params.ApiConnectionDetails == nil {
		return ""
	}

	config := a.Params.ApiConnectionDetails
	if username := strings.TrimSpace(config.BasicAuth.Username); username != "" {
		return username
	}
	if username := strings.TrimSpace(config.OpenIdPasswordAuth.Username); username != "" {
		return username
	}
	if !config.KubernetesNativeAuth.Enabled && config.OpenIdAuth.ProviderUrl == "" && config.OpenIdDeviceAuth.ProviderUrl == "" && config.OpenIdClientCredentialsAuth.ProviderUrl == "" && config.OpenIdKubernetesAuth.ProviderUrl == "" && config.ExecAuth.Cmd == "" {
		return "anonymous"
	}
	return ""
}

func (a *App) printActionUser() {
	if user := a.actionUser(); user != "" {
		fmt.Fprintf(a.Out, "user: %s\n", user)
	}
}
