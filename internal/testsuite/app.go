package testsuite

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/hashicorp/go-multierror"
	"github.com/mattn/go-zglob"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"github.com/renstrom/shortuuid"
	"golang.org/x/sync/errgroup"
	apimachineryYaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"

	"github.com/armadaproject/armada/internal/testsuite/build"
	"github.com/armadaproject/armada/internal/testsuite/eventbenchmark"
	"github.com/armadaproject/armada/internal/testsuite/eventlogger"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

const metricsPrefix = "armada_testsuite_"

type App struct {
	// Parameters passed to the CLI by the user.
	Params *Params
	// Out is used to write the output. Defaults to standard out,
	// but can be overridden in tests to make assertions on the applications's output.
	Out io.Writer
	// Source of randomness. Tests can use a mocked random source in order to provide
	// deterministic testing behaviour.
	Random io.Reader
}

// Params struct holds all user-customizable parameters.
// Using a single struct for all CLI commands ensures that all flags are distinct
// and that they can be provided either dynamically on a command line, or
// statically in a config file that's reused between command runs.
type Params struct {
	// Armada connection details.
	ApiConnectionDetails *client.ApiConnectionDetails
	// If non-empty, push metrics containing test results to a Prometheus push gateway with this url.
	PrometheusPushGatewayUrl string
	// Exported metrics are annotated with job=PrometheusPushGatewayJobName.
	// Must be non-empty.
	PrometheusPushGatewayJobName string
	// ConfigHook applies the config overrides of test cases. When it is nil, the testsuite skips test cases with overrides.
	ConfigHook ConfigHook
}

// New instantiates an App with default parameters, including standard output
// and cryptographically secure random source.
func New() *App {
	return &App{
		Params: &Params{},
		Out:    os.Stdout,
		Random: rand.Reader,
	}
}

// Version prints build information (e.g., current git commit) to the app output.
func (a *App) Version() error {
	w := tabwriter.NewWriter(a.Out, 1, 1, 1, ' ', 0)
	defer w.Flush()
	_, _ = fmt.Fprintf(w, "Version:\t%s\n", build.ReleaseVersion)
	_, _ = fmt.Fprintf(w, "Commit:\t%s\n", build.GitCommit)
	_, _ = fmt.Fprintf(w, "Go version:\t%s\n", build.GoVersion)
	_, _ = fmt.Fprintf(w, "Built:\t%s\n", build.BuildTime)
	return nil
}

func (a *App) TestPattern(ctx context.Context, pattern string) (*TestSuiteReport, error) {
	testSpecs, err := TestSpecsFromPattern(pattern)
	if err != nil {
		return nil, err
	}
	return a.RunTests(ctx, testSpecs)
}

func TestSpecsFromPattern(pattern string) ([]*api.TestSpec, error) {
	var allPaths []string
	for _, p := range strings.Split(pattern, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		paths, err := zglob.Glob(p)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to glob test specs with pattern %s", p)
		}
		for _, path := range paths {
			ext := strings.ToLower(filepath.Ext(path))
			if ext == ".yaml" || ext == ".yml" {
				allPaths = append(allPaths, path)
			}
		}
	}
	return TestSpecsFromFilePaths(allPaths)
}

func TestSpecsFromFilePaths(filePaths []string) ([]*api.TestSpec, error) {
	rv := make([]*api.TestSpec, len(filePaths))
	for i, filePath := range filePaths {
		testSpec, err := TestSpecFromFilePath(filePath)
		if err != nil {
			return nil, err
		}
		rv[i] = testSpec
	}
	return rv, nil
}

func TestSpecFromFilePath(filePath string) (*api.TestSpec, error) {
	testSpec := &api.TestSpec{}
	yamlBytes, err := os.ReadFile(filePath)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if err := UnmarshalTestCase(yamlBytes, testSpec); err != nil {
		return nil, errors.WithMessagef(err, "Could not parse file %s", filePath)
	}

	// Randomise job set for each test to ensure we're only getting events for this run.
	fileName := filepath.Base(filePath)
	fileName = strings.TrimSuffix(fileName, filepath.Ext(fileName))
	testSpec.JobSetId = fileName + "-" + shortuuid.New()

	// If no test name is provided, set it to be the filename.
	if testSpec.Name == "" {
		testSpec.Name = fileName
	}

	return testSpec, nil
}

type TestSuiteReport struct {
	Start           time.Time
	Finish          time.Time
	TestCaseReports []*TestCaseReport
}

func (tsr *TestSuiteReport) Describe(c chan<- *prometheus.Desc) {
	for _, tcr := range tsr.TestCaseReports {
		tcr.Describe(c)
	}
}

func (tsr *TestSuiteReport) Collect(c chan<- prometheus.Metric) {
	for _, tcr := range tsr.TestCaseReports {
		tcr.Collect(c)
	}
}

type TestCaseReport struct {
	Out           *bytes.Buffer
	Start         time.Time
	Finish        time.Time
	FailureReason string
	// SkipReason says why the testsuite skipped the test case.
	SkipReason      string
	BenchmarkReport *eventbenchmark.TestCaseBenchmarkReport
	TestSpec        *api.TestSpec

	// Prometheus metric descriptions.
	// Test start time in seconds since the epoch.
	startTimePrometheusDesc *prometheus.Desc
	// Test finish time in seconds since the epoch.
	finishTimePrometheusDesc *prometheus.Desc
	// Outputs 1 on test timeout.
	testTimeoutPrometheusDesc *prometheus.Desc
	// Outputs 1 on test failure, not including timeouts.
	testFailurePrometheusDesc *prometheus.Desc
}

func NewTestCaseReport(testSpec *api.TestSpec) *TestCaseReport {
	rv := &TestCaseReport{
		Start:    time.Now(),
		TestSpec: testSpec,
	}
	rv.initialiseMetrics()
	return rv
}

func (r *TestCaseReport) initialiseMetrics() {
	r.startTimePrometheusDesc = prometheus.NewDesc(
		metricsPrefix+"test_start_time",
		"The time at which a test started.",
		[]string{"testcase", "environment", "target"},
		nil,
	)
	r.finishTimePrometheusDesc = prometheus.NewDesc(
		metricsPrefix+"test_finish_time",
		"The time at which a test finished.",
		[]string{"testcase", "environment", "target"},
		nil,
	)
	r.testTimeoutPrometheusDesc = prometheus.NewDesc(
		metricsPrefix+"test_timeout",
		"Outputs 1 on test timeout and 0 otherwise.",
		[]string{"testcase", "environment", "target"},
		nil,
	)
	r.testFailurePrometheusDesc = prometheus.NewDesc(
		metricsPrefix+"test_failure",
		"Outputs 1 on test failure, not including timeout, and 0 otherwise.",
		[]string{"testcase", "environment", "target"},
		nil,
	)
}

func (r *TestCaseReport) Describe(c chan<- *prometheus.Desc) {
	c <- r.startTimePrometheusDesc
	c <- r.finishTimePrometheusDesc
	c <- r.testTimeoutPrometheusDesc
	c <- r.testFailurePrometheusDesc
}

func (r *TestCaseReport) Collect(c chan<- prometheus.Metric) {
	c <- prometheus.MustNewConstMetric(
		r.startTimePrometheusDesc,
		prometheus.CounterValue,
		float64(r.Start.Unix()),
		r.TestSpec.Name,
		r.TestSpec.Environment,
		r.TestSpec.Target,
	)
	c <- prometheus.MustNewConstMetric(
		r.finishTimePrometheusDesc,
		prometheus.CounterValue,
		float64(r.Finish.Unix()),
		r.TestSpec.Name,
		r.TestSpec.Environment,
		r.TestSpec.Target,
	)

	// Test failures always contain either "unexpected event for job" or "error asserting failure reason".
	// TODO(albin): Improve this.
	testFailure := 0.0
	if strings.Contains(r.FailureReason, "unexpected event for job") || strings.Contains(r.FailureReason, "error asserting failure reason") || strings.Contains(r.FailureReason, "expected categories") {
		testFailure = 1.0
	}
	c <- prometheus.MustNewConstMetric(
		r.testFailurePrometheusDesc,
		prometheus.GaugeValue,
		testFailure,
		r.TestSpec.Name,
		r.TestSpec.Environment,
		r.TestSpec.Target,
	)

	// We assume that any other failures are due to timeout.
	// TODO(albin): Improve this.
	testTimeout := 0.0
	if r.FailureReason != "" && testFailure == 0 {
		testTimeout = 1.0
	}
	c <- prometheus.MustNewConstMetric(
		r.testTimeoutPrometheusDesc,
		prometheus.GaugeValue,
		testTimeout,
		r.TestSpec.Name,
		r.TestSpec.Environment,
		r.TestSpec.Target,
	)
}

func (report *TestSuiteReport) NumSuccesses() int {
	if report == nil {
		return 0
	}
	rv := 0
	for _, r := range report.TestCaseReports {
		if r != nil && r.FailureReason == "" && r.SkipReason == "" {
			rv++
		}
	}
	return rv
}

func (report *TestSuiteReport) NumSkipped() int {
	if report == nil {
		return 0
	}
	rv := 0
	for _, r := range report.TestCaseReports {
		if r != nil && r.SkipReason != "" {
			rv++
		}
	}
	return rv
}

func (report *TestSuiteReport) NumFailures() int {
	if report == nil {
		return 0
	}
	rv := 0
	for _, r := range report.TestCaseReports {
		if r != nil && r.FailureReason != "" {
			rv++
		}
	}
	return rv
}

func (a *App) RunTests(ctx context.Context, testSpecs []*api.TestSpec) (*TestSuiteReport, error) {
	rv := &TestSuiteReport{
		Start:           time.Now(),
		TestCaseReports: make([]*TestCaseReport, len(testSpecs)),
	}
	defer func() {
		rv.Finish = time.Now()
	}()

	groups, err := groupByConfig(testSpecs)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	g, ctx := errgroup.WithContext(ctx)
	eventLogger := eventlogger.New(make(chan *api.EventMessage), time.Second)
	eventLogger.Out = a.Out
	g.Go(func() error { return eventLogger.Run(ctx) })

	// An interrupted run can leave overrides applied, and the test cases without overrides must not inherit them.
	if a.Params.ConfigHook != nil {
		if err := applyConfig(ctx, a.Params.ConfigHook, nil); err != nil {
			return nil, errors.WithMessage(err, "failed to remove config overrides before the test run")
		}
	}

	applied := false
	var restoreErr error
	for _, group := range groups {
		if group.key == "" {
			a.runConcurrently(ctx, testSpecs, group.indexes, eventLogger, rv)
			continue
		}
		if a.Params.ConfigHook == nil {
			for _, i := range group.indexes {
				rv.TestCaseReports[i] = finishedTestCaseReport(testSpecs[i], "", "the test case has config overrides, and no config hook is set")
			}
			continue
		}
		fmt.Fprintf(a.Out, "applying config overrides %s\n", group.key)
		applied = true
		if err := applyConfig(ctx, a.Params.ConfigHook, group.config); err != nil {
			for _, i := range group.indexes {
				rv.TestCaseReports[i] = finishedTestCaseReport(testSpecs[i], fmt.Sprintf("failed to apply config overrides: %s", err), "")
			}
			continue
		}
		a.runConcurrently(ctx, testSpecs, group.indexes, eventLogger, rv)
	}
	if applied {
		fmt.Fprintf(a.Out, "removing config overrides\n")
		// A signal cancels ctx, and the overrides must still go, so the removal gets its own context.
		restoreCtx, cancelRestore := context.WithTimeout(context.WithoutCancel(ctx), configRestoreTimeout)
		defer cancelRestore()
		if err := applyConfig(restoreCtx, a.Params.ConfigHook, nil); err != nil {
			restoreErr = errors.WithMessage(err, "failed to remove config overrides")
		}
	}

	cancel()
	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Optionally push metrics.
	if a.Params.PrometheusPushGatewayUrl != "" {
		if err := pushTestSuiteReportMetrics(rv, a.Params.PrometheusPushGatewayUrl, a.Params.PrometheusPushGatewayJobName); err != nil {
			return nil, err
		}
	}
	// The report stays valid when the hook cannot remove the overrides, so the caller can still write it.
	return rv, restoreErr
}

// runConcurrently runs the test cases at the given indexes at the same time, and waits until all of them finish.
func (a *App) runConcurrently(ctx context.Context, testSpecs []*api.TestSpec, indexes []int, eventLogger *eventlogger.EventLogger, rv *TestSuiteReport) {
	wg := sync.WaitGroup{}
	wg.Add(len(indexes))
	for _, i := range indexes {
		testRunner := TestRunner{
			Out:                  a.Out,
			apiConnectionDetails: a.Params.ApiConnectionDetails,
			testSpec:             testSpecs[i],
			eventLogger:          eventLogger,
		}
		go func() {
			_ = testRunner.Run(ctx)
			rv.TestCaseReports[i] = testRunner.TestCaseReport
			wg.Done()
		}()
		time.Sleep(100 * time.Millisecond)
	}
	fmt.Fprintf(a.Out, "---\n")
	wg.Wait()
}

// finishedTestCaseReport returns the report of a test case that did not run.
func finishedTestCaseReport(testSpec *api.TestSpec, failureReason string, skipReason string) *TestCaseReport {
	report := NewTestCaseReport(testSpec)
	report.Finish = report.Start
	report.FailureReason = failureReason
	report.SkipReason = skipReason
	report.Out = bytes.NewBufferString(failureReason + skipReason + "\n")
	return report
}

func pushTestSuiteReportMetrics(tsr *TestSuiteReport, url, job string) error {
	pusher := push.New(url, job)
	pusher.Collector(tsr)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := pusher.PushContext(ctx); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// UnmarshalTestCase unmarshalls bytes into a TestSpec.
func UnmarshalTestCase(yamlBytes []byte, testSpec *api.TestSpec) error {
	var result *multierror.Error
	successExpectedEvents := false
	successEverythingElse := false
	yamlSpecSeparator := []byte("\n---\n")
	docs := bytes.Split(yamlBytes, yamlSpecSeparator)

	for _, docYamlBytes := range docs {

		// yaml.Unmarshal can unmarshal everything,
		// but leaves oneof fields empty (these are silently discarded).

		if err := apimachineryYaml.NewYAMLOrJSONDecoder(bytes.NewReader(yamlBytes), 128).Decode(testSpec); err != nil {
			result = multierror.Append(result, err)
		} else {
			successEverythingElse = true
		}

		// YAMLToJSON + jsonpb.Unmarshaler can unmarshal oneof fields,
		// but can't unmarshal k8s pod specs.
		docJsonBytes, err := yaml.YAMLToJSON(docYamlBytes)
		if err != nil {
			result = multierror.Append(result, err)
			continue
		}
		unmarshaler := jsonpb.Unmarshaler{AllowUnknownFields: true}
		err = unmarshaler.Unmarshal(bytes.NewReader(docJsonBytes), testSpec)
		if err != nil {
			result = multierror.Append(result, err)
		} else {
			successExpectedEvents = true
		}
	}
	if !successExpectedEvents || !successEverythingElse {
		return result.ErrorOrNil()
	}
	// The YAML decoder leaves the config values empty, and the jsonpb decoder fails on a document with pod specs.
	// So config has its own decoder.
	config, err := configFromDocs(docs)
	if err != nil {
		return err
	}
	testSpec.Config = config
	return nil
}
