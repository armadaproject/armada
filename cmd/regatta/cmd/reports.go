package cmd

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	log "github.com/armadaproject/armada/internal/common/logging"
	regattaconfig "github.com/armadaproject/armada/internal/regatta/config"
	"github.com/armadaproject/armada/internal/regatta/metrics"
)

// Reports are collected on a fresh context, not the run's, so the last report of an interrupted run can
// still be written.
const reportCollectTimeout = 2 * time.Minute

// reportFileName returns regatta-result-<timestamp>.json for a bounded run, and for a continuous run
// regatta-result-<timestamp>-00001.json, -00002.json, ..., the last ending in -final.
func reportFileName(timestamp string, sequence int, final bool) string {
	switch {
	case final:
		return fmt.Sprintf("regatta-result-%s-%05d-final.json", timestamp, sequence)
	case sequence > 0:
		return fmt.Sprintf("regatta-result-%s-%05d.json", timestamp, sequence)
	default:
		return fmt.Sprintf("regatta-result-%s.json", timestamp)
	}
}

type reportInputs struct {
	scenario          *regattaconfig.Scenario
	queues            []string
	readinessFailures []metrics.ReadinessFailure
	resultsPath       string
}

// write logs errors instead of returning them: a failed report must not take down a run that otherwise
// succeeded.
func (in reportInputs) write(name string, start, end time.Time) {
	ctx, cancel := context.WithTimeout(context.Background(), reportCollectTimeout)
	defer cancel()

	report, err := metrics.Collect(ctx, in.scenario.PrometheusURL(), in.queues, start, end)
	if err != nil {
		log.Errorf("collecting metrics report: %s", err)
		return
	}
	report.Scenario = in.scenario
	report.ReadinessFailures = in.readinessFailures
	outputPath := filepath.Join(in.resultsPath, name)
	if err := report.WriteJSON(outputPath); err != nil {
		log.Errorf("writing metrics report: %s", err)
		return
	}
	log.Infof("metrics report (%s to %s) written to %s", start.Format("15:04:05"), end.Format("15:04:05"), outputPath)
}

// runContinuously submits until ctx is cancelled (the first interrupt), writing a report every reportInterval
// and a final one when it stops. Each report covers the time since the previous one and ends settle (the
// post-run delay) before it is written, so the metrics have reached Prometheus.
func runContinuously(ctx context.Context, in reportInputs, submitUntilStopped func(context.Context) error, timestamp string, start time.Time) error {
	interval := in.scenario.Metrics.ReportIntervalDuration
	settle := in.scenario.Metrics.PostRunDelayDuration

	submitted := make(chan error, 1)
	go func() { submitted <- submitUntilStopped(ctx) }()

	log.Infof("continuous run: a report every %s covering the time since the last one, and a final one on interrupt (Ctrl+C)", interval)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	windowStart := start
	sequence := 0
	for {
		select {
		case <-ticker.C:
			end := time.Now().Add(-settle)
			if !end.After(windowStart) {
				continue // the interval is shorter than the settle delay: nothing new to report yet
			}
			sequence++
			in.write(reportFileName(timestamp, sequence, false), windowStart, end)
			windowStart = end

		case err := <-submitted:
			if err != nil {
				return err
			}
			stopped := time.Now()
			log.Infof("submission stopped; waiting %s for Prometheus to catch up before the final report (Ctrl+C again to quit without it)", settle)
			time.Sleep(settle)
			sequence++
			in.write(reportFileName(timestamp, sequence, true), windowStart, stopped)
			return nil
		}
	}
}
