package kwok

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

// execPrinting returns an exec credential plugin config whose command prints the given ExecCredential status.
func execPrinting(status string) *clientcmdapi.ExecConfig {
	output := `{"apiVersion":"client.authentication.k8s.io/v1beta1","kind":"ExecCredential","status":` + status + `}`
	return &clientcmdapi.ExecConfig{Command: "sh", Args: []string{"-c", "printf '%s' '" + output + "'"}}
}

func TestResolveExecCredential(t *testing.T) {
	t.Run("a token-only credential, such as aws eks get-token, is accepted", func(t *testing.T) {
		authInfo := &clientcmdapi.AuthInfo{Exec: execPrinting(`{"token":"abc","expirationTimestamp":"2099-01-01T00:00:00Z"}`)}
		require.NoError(t, resolveExecCredential(context.Background(), authInfo))
		require.Equal(t, "abc", authInfo.Token)
		require.Nil(t, authInfo.Exec, "the plugin is replaced by what it returned")
		require.Empty(t, authInfo.ClientCertificateData)
	})
	t.Run("a client certificate and key are accepted", func(t *testing.T) {
		authInfo := &clientcmdapi.AuthInfo{Exec: execPrinting(`{"clientCertificateData":"CERT","clientKeyData":"KEY"}`)}
		require.NoError(t, resolveExecCredential(context.Background(), authInfo))
		require.Equal(t, []byte("CERT"), authInfo.ClientCertificateData)
		require.Equal(t, []byte("KEY"), authInfo.ClientKeyData)
		require.Empty(t, authInfo.Token)
	})
	t.Run("a certificate without its key is not enough", func(t *testing.T) {
		authInfo := &clientcmdapi.AuthInfo{Exec: execPrinting(`{"clientCertificateData":"CERT"}`)}
		require.ErrorContains(t, resolveExecCredential(context.Background(), authInfo), "neither a token nor a client certificate")
	})
	t.Run("a response with no status is an error", func(t *testing.T) {
		authInfo := &clientcmdapi.AuthInfo{Exec: &clientcmdapi.ExecConfig{
			Command: "sh", Args: []string{"-c", `printf '%s' '{"apiVersion":"client.authentication.k8s.io/v1beta1","kind":"ExecCredential"}'`},
		}}
		require.ErrorContains(t, resolveExecCredential(context.Background(), authInfo), "no status")
	})
}

func TestBuildInternalKubeconfig(t *testing.T) {
	dir := t.TempDir()
	for name, content := range map[string]string{"ca.crt": "CA-PEM", "client.crt": "CERT-PEM", "client.key": "KEY-PEM"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600))
	}
	write := func(config *clientcmdapi.Config) string {
		path := filepath.Join(dir, "kubeconfig")
		require.NoError(t, clientcmd.WriteToFile(*config, path))
		return path
	}
	build := func(t *testing.T, path string) *clientcmdapi.Config {
		out, err := buildInternalKubeconfig(context.Background(), path, "https://internal.example:6443")
		require.NoError(t, err)
		built, err := clientcmd.Load(out)
		require.NoError(t, err)
		return built
	}

	t.Run("certificate files are embedded, relative paths included, and only the current context is kept", func(t *testing.T) {
		config := clientcmdapi.NewConfig()
		config.Clusters["mine"] = &clientcmdapi.Cluster{Server: "https://host.example:6443", CertificateAuthority: "ca.crt"} // relative to the kubeconfig
		config.AuthInfos["me"] = &clientcmdapi.AuthInfo{ClientCertificate: "client.crt", ClientKey: filepath.Join(dir, "client.key")}
		config.Contexts["mine"] = &clientcmdapi.Context{Cluster: "mine", AuthInfo: "me"}
		config.Clusters["other"] = &clientcmdapi.Cluster{Server: "https://other.example:6443", CertificateAuthority: "/does/not/exist.crt"}
		config.AuthInfos["someone-else"] = &clientcmdapi.AuthInfo{Token: "OTHER-SECRET"}
		config.Contexts["other"] = &clientcmdapi.Context{Cluster: "other", AuthInfo: "someone-else"}
		config.CurrentContext = "mine"

		built := build(t, write(config))

		cluster := built.Clusters["mine"]
		require.Equal(t, "https://internal.example:6443", cluster.Server)
		require.Equal(t, []byte("CA-PEM"), cluster.CertificateAuthorityData)
		require.Empty(t, cluster.CertificateAuthority, "no host path is left for the container to miss")
		user := built.AuthInfos["me"]
		require.Equal(t, []byte("CERT-PEM"), user.ClientCertificateData)
		require.Equal(t, []byte("KEY-PEM"), user.ClientKeyData)
		require.Empty(t, user.ClientCertificate)
		require.Empty(t, user.ClientKey)
		require.Len(t, built.Contexts, 1)
		require.Len(t, built.AuthInfos, 1, "another context's credentials are not copied into the file")
		require.NotContains(t, built.AuthInfos, "someone-else")
	})

	t.Run("an exec plugin that returns a token is resolved into the file", func(t *testing.T) {
		config := clientcmdapi.NewConfig()
		config.Clusters["c"] = &clientcmdapi.Cluster{Server: "https://host.example:6443", CertificateAuthorityData: []byte("CA")}
		config.AuthInfos["u"] = &clientcmdapi.AuthInfo{Exec: execPrinting(`{"token":"eks-token"}`)}
		config.Contexts["c"] = &clientcmdapi.Context{Cluster: "c", AuthInfo: "u"}
		config.CurrentContext = "c"

		built := build(t, write(config))

		require.Equal(t, "eks-token", built.AuthInfos["u"].Token)
		require.Nil(t, built.AuthInfos["u"].Exec)
	})

	t.Run("a missing certificate file is an error", func(t *testing.T) {
		config := clientcmdapi.NewConfig()
		config.Clusters["c"] = &clientcmdapi.Cluster{Server: "https://host.example:6443", CertificateAuthority: "gone.crt"}
		config.AuthInfos["u"] = &clientcmdapi.AuthInfo{Token: "t"}
		config.Contexts["c"] = &clientcmdapi.Context{Cluster: "c", AuthInfo: "u"}
		config.CurrentContext = "c"

		_, err := buildInternalKubeconfig(context.Background(), write(config), "https://internal.example:6443")
		require.ErrorContains(t, err, "embedding certificate files")
	})
}

func TestWriteControllerKubeconfig(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CACHE_HOME", filepath.Join(home, ".cache"))

	t.Run("the file is private, in a private directory under the user's cache", func(t *testing.T) {
		path, err := writeControllerKubeconfig("gpu", []byte("secret"))
		require.NoError(t, err)
		cache, err := os.UserCacheDir()
		require.NoError(t, err)
		require.Equal(t, filepath.Join(cache, "regatta", "kwok-gpu", "kubeconfig"), path)
		if runtime.GOOS != "windows" {
			info, err := os.Stat(path)
			require.NoError(t, err)
			require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
			dirInfo, err := os.Stat(filepath.Dir(path))
			require.NoError(t, err)
			require.Equal(t, os.FileMode(0o700), dirInfo.Mode().Perm())
		}
		content, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, "secret", string(content))
	})

	t.Run("an earlier, more permissive file is replaced rather than reused", func(t *testing.T) {
		path, err := writeControllerKubeconfig("cpu", []byte("first"))
		require.NoError(t, err)
		require.NoError(t, os.Chmod(path, 0o666))

		path, err = writeControllerKubeconfig("cpu", []byte("second"))
		require.NoError(t, err)
		info, err := os.Stat(path)
		require.NoError(t, err)
		if runtime.GOOS != "windows" {
			require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
		}
		content, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, "second", string(content))
	})

	t.Run("a directory that is really a symlink is refused", func(t *testing.T) {
		dir, err := controllerKubeconfigDir("linked")
		require.NoError(t, err)
		require.NoError(t, os.MkdirAll(filepath.Dir(dir), 0o700))
		elsewhere := t.TempDir()
		require.NoError(t, os.Symlink(elsewhere, dir))

		_, err = writeControllerKubeconfig("linked", []byte("secret"))
		require.Error(t, err)
		entries, err := os.ReadDir(elsewhere)
		require.NoError(t, err)
		require.Empty(t, entries, "nothing was written through the link")
	})

	t.Run("target names that are not a single path element are refused", func(t *testing.T) {
		for _, name := range []string{"", "..", ".", "a/b", "../x"} {
			_, err := writeControllerKubeconfig(name, []byte("x"))
			require.Error(t, err, name)
		}
	})
}

func TestTeardownController_RunningItAgainWhenNothingIsLeftIsNotAnError(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker is not available")
	}
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skip("the docker daemon is not reachable")
	}
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CACHE_HOME", filepath.Join(home, ".cache"))

	// No container of this name exists: `docker rm -f` reports "No such container" and still exits 0, so a second
	// teardown, or one for a target that never started, finishes cleanly.
	require.NoError(t, TeardownController(context.Background(), "teardown-test-never-started"))
	require.NoError(t, TeardownController(context.Background(), "teardown-test-never-started"))
}
