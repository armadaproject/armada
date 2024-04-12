package cmd

import (
	"bufio"
	"bytes"
	"github.com/armadaproject/armada/pkg/client"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var configPath = "./testdata/test_config.yaml"
var configCopyPath = "./testdata/test_config_copy.yaml"

func setUp(tb testing.TB) func(testing.TB) {
	srcFile, err := os.Open(configPath)
	if err != nil {
		tb.Error(err)
	}
	defer srcFile.Close()

	dstFile, err := os.Create(configCopyPath)
	if err != nil {
		tb.Error(err)
	}
	defer dstFile.Close()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		tb.Error(err)
	}

	if err := dstFile.Sync(); err != nil {
		tb.Error(err)
	}

	return func(tb testing.TB) {
		if err := os.Remove(configCopyPath); err != nil {
			tb.Errorf("failed to delete copied configuration file: %s", err)
		}
	}

}

func TestUseContext(t *testing.T) {
	tearDown := setUp(t)
	defer tearDown(t)

	t.Run("use-context", func(t *testing.T) {
		cmd := RootCmd()
		args := []string{"config", "use-context", "test", "--config", configCopyPath}
		cmd.SetArgs(args)

		require.NoError(t, cmd.Execute())

		cfg, err := client.ReadConfigFromPath(configCopyPath)
		require.NoError(t, err)
		require.Equal(t, "test", cfg.CurrentContext)
	})
}

func TestGetContexts(t *testing.T) {
	t.Run("get-context", func(t *testing.T) {
		stdOut := os.Stdout
		defer func() { os.Stdout = stdOut }()

		r, w, _ := os.Pipe()
		os.Stdout = w
		cmd := RootCmd()
		args := []string{"config", "get-contexts", "--config", configPath}
		cmd.SetArgs(args)

		require.NoError(t, cmd.Execute())

		w.Close()

		var buf bytes.Buffer
		for scanner := bufio.NewScanner(r); scanner.Scan(); buf.Write(scanner.Bytes()) {
		}

		require.Equal(t, "Available contexts: main (current), test", buf.String())
	})
}

func TestCurrentContext(t *testing.T) {
	t.Run("current-context", func(t *testing.T) {
		stdOut := os.Stdout
		defer func() { os.Stdout = stdOut }()

		r, w, _ := os.Pipe()
		os.Stdout = w
		cmd := RootCmd()
		args := []string{"config", "current-context", "--config", configPath}
		cmd.SetArgs(args)

		require.NoError(t, cmd.Execute())

		w.Close()

		var buf bytes.Buffer
		for scanner := bufio.NewScanner(r); scanner.Scan(); buf.Write(scanner.Bytes()) {
		}

		require.Equal(t, "Current context: main", buf.String())
	})
}
