package kwok

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

const testKubeconfig = `apiVersion: v1
kind: Config
current-context: kind-armada-cluster-2
contexts:
- name: kind-armada-cluster-1
  context: {cluster: kind-armada-cluster-1, user: u}
- name: kind-armada-cluster-2
  context: {cluster: kind-armada-cluster-2, user: u}
clusters:
- name: kind-armada-cluster-1
  cluster: {server: "https://0.0.0.0:59855"}
- name: kind-armada-cluster-2
  cluster: {server: "https://0.0.0.0:59870"}
users:
- name: u
  user: {}
`

func TestDescribeKubeconfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config")
	require.NoError(t, os.WriteFile(path, []byte(testKubeconfig), 0o600))

	contextName, server, err := DescribeKubeconfig(path)
	require.NoError(t, err)
	require.Equal(t, "kind-armada-cluster-2", contextName, "reports the file's current-context, which is what regatta trusts")
	require.Equal(t, "https://0.0.0.0:59870", server)
}

func TestDescribeKubeconfig_MissingFile(t *testing.T) {
	_, _, err := DescribeKubeconfig(filepath.Join(t.TempDir(), "nope"))
	require.Error(t, err)
}

func TestParseContainerState(t *testing.T) {
	running, exit, err := parseContainerState("true 0\n")
	require.NoError(t, err)
	require.True(t, running)
	require.Equal(t, 0, exit)

	running, exit, err = parseContainerState("false 1")
	require.NoError(t, err)
	require.False(t, running, "a controller that died at startup is reported as not running")
	require.Equal(t, 1, exit)

	for _, bad := range []string{"", "true", "maybe 0", "true x", "true 0 extra"} {
		_, _, err := parseContainerState(bad)
		require.Error(t, err, bad)
	}
}
