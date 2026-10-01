package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"

	regattaconfig "github.com/armadaproject/armada/internal/regatta/config"
	"github.com/armadaproject/armada/pkg/client"
)

const testArmadactlConfig = `currentContext: main
contexts:
  main:
    armadaUrl: "localhost:50051"
  dev:
    armadaUrl: "armada.example.com:443"
`

func TestLoadArmadaConnection(t *testing.T) {
	tests := map[string]struct {
		authContext    string
		contextFlagSet bool
		wantUrl        string
		wantErr        string
	}{
		"empty authContext uses the config file's currentContext": {
			wantUrl: "localhost:50051",
		},
		"authContext selects another context": {
			authContext: "dev",
			wantUrl:     "armada.example.com:443",
		},
		"authContext naming the current context": {
			authContext: "main",
			wantUrl:     "localhost:50051",
		},
		"unknown authContext errors and lists the available contexts": {
			authContext: "nope",
			wantErr:     "available: ",
		},
		"--context flag takes precedence over authContext": {
			authContext:    "dev",
			contextFlagSet: true,
			wantUrl:        "localhost:50051",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			viper.Reset()
			t.Cleanup(viper.Reset)

			// Register the same connection flags as the real root command: the url extraction
			// compares viper's armadaUrl against the flag's default, so they must be bound.
			root := &cobra.Command{}
			client.AddArmadaApiConnectionCommandlineArgs(root)
			if tc.contextFlagSet {
				require.NoError(t, root.PersistentFlags().Set("context", "main"))
			}

			path := filepath.Join(t.TempDir(), ".armadactl.yaml")
			require.NoError(t, os.WriteFile(path, []byte(testArmadactlConfig), 0o600))

			details, err := loadArmadaConnection(
				&regattaconfig.Scenario{Armadactl: path, AuthContext: tc.authContext},
				tc.contextFlagSet,
			)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantUrl, details.ArmadaUrl)
		})
	}
}
