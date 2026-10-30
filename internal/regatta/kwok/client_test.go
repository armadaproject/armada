package kwok

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	regattaconfig "github.com/armadaproject/armada/internal/regatta/config"
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

func TestBuildRestConfig_BoundsEveryRequest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kubeconfig")
	config := clientcmdapi.NewConfig()
	config.Clusters["c"] = &clientcmdapi.Cluster{Server: "https://api.example:6443"}
	config.AuthInfos["u"] = &clientcmdapi.AuthInfo{Token: "t"}
	config.Contexts["c"] = &clientcmdapi.Context{Cluster: "c", AuthInfo: "u"}
	config.CurrentContext = "c"
	require.NoError(t, clientcmd.WriteToFile(*config, path))

	restConfig, err := buildRestConfig(path, regattaconfig.KubernetesClientConfiguration{})

	require.NoError(t, err)
	require.Equal(t, kubeRequestTimeout, restConfig.Timeout, "a stalled API server cannot block a call forever")
}

func TestKubeconfigServerAddress(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kubeconfig")
	require.NoError(t, os.WriteFile(path, []byte(testKubeconfig), 0o600))

	server, err := KubeconfigServerAddress(path)
	require.NoError(t, err)
	require.Equal(t, "https://0.0.0.0:59870", server, "the current context's cluster")

	t.Setenv("KUBECONFIG", path)
	server, err = KubeconfigServerAddress("")
	require.NoError(t, err, "an empty path is the default kubeconfig, as for the Kubernetes client")
	require.Equal(t, "https://0.0.0.0:59870", server)
}
