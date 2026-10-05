package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

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

func TestReportFileName(t *testing.T) {
	require.Equal(t, "regatta-result-20261005-120000.json", reportFileName("20261005-120000", 0, false), "a bounded run's single report")
	require.Equal(t, "regatta-result-20261005-120000-00001.json", reportFileName("20261005-120000", 1, false))
	require.Equal(t, "regatta-result-20261005-120000-00012.json", reportFileName("20261005-120000", 12, false))
	require.Equal(t, "regatta-result-20261005-120000-00003-final.json", reportFileName("20261005-120000", 3, true), "the last report of a continuous run")

	// Lexical order is write order, well past a day and a half of two-minute reports.
	for i := 1; i < 20000; i++ {
		require.Less(t, reportFileName("t", i, false), reportFileName("t", i+1, false))
	}
}

func TestRunContinuously_WritesContiguousReportsThenAFinalOne(t *testing.T) {
	prometheus := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `{"data":{"result":[{"value":[1,"5"]}]}}`)
	}))
	defer prometheus.Close()

	dir := t.TempDir()
	in := reportInputs{
		scenario: &regattaconfig.Scenario{Metrics: regattaconfig.MetricsConfig{
			Prometheus:             prometheus.URL,
			ReportIntervalDuration: 120 * time.Millisecond,
			PostRunDelayDuration:   20 * time.Millisecond,
		}},
		queues:      []string{"q-1"},
		resultsPath: dir,
	}

	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	start := time.Now()
	done := make(chan error, 1)
	go func() {
		done <- runContinuously(ctx, in, func(ctx context.Context) error {
			<-ctx.Done()
			close(stopped)
			return nil
		}, "20261005-120000", start)
	}()
	time.Sleep(450 * time.Millisecond)
	cancel()
	require.NoError(t, <-done)

	files, err := filepath.Glob(filepath.Join(dir, "regatta-result-20261005-120000-*.json"))
	require.NoError(t, err)
	sort.Strings(files)
	require.GreaterOrEqual(t, len(files), 3, "at least two periodic reports and the final one")
	require.True(t, strings.HasSuffix(files[len(files)-1], "-final.json"), "the last file is the final report: %v", files)
	for _, f := range files[:len(files)-1] {
		require.False(t, strings.Contains(f, "final"), "only the last is final: %s", f)
	}

	type window struct{ Start, End time.Time }
	var previous window
	for i, f := range files {
		data, err := os.ReadFile(f)
		require.NoError(t, err)
		var w window
		require.NoError(t, json.Unmarshal(data, &w))
		require.True(t, w.End.After(w.Start), "%s: a window has a length", f)
		if i == 0 {
			require.WithinDuration(t, start, w.Start, time.Millisecond, "the first report starts when the run does")
		} else {
			require.True(t, previous.End.Equal(w.Start), "%s starts where the previous report ended, so no time is missed or counted twice", f)
		}
		previous = w
	}
}

func TestRunContinuously_SubmissionFailureIsReturned(t *testing.T) {
	in := reportInputs{scenario: &regattaconfig.Scenario{Metrics: regattaconfig.MetricsConfig{
		ReportIntervalDuration: time.Hour, PostRunDelayDuration: time.Millisecond,
	}}}
	err := runContinuously(context.Background(), in, func(context.Context) error { return errors.New("server down") }, "t", time.Now())
	require.ErrorContains(t, err, "server down")
}
