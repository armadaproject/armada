package testsuite

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"io/ioutil"
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
	ApiConnectionDetails *client.ApiConnectionDetails
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
	filePaths, err := zglob.Glob(pattern)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return TestSpecsFromFilePaths(filePaths)
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
	yamlBytes, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if err := UnmarshalTestCase(yamlBytes, testSpec); err != nil {
		return nil, err
	}

	// Randomise jobSetName for each test to ensure we're only getting events for this run.
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

type TestCaseReport struct {
	Out             *bytes.Buffer
	Start           time.Time
	Finish          time.Time
	FailureReason   string
	BenchmarkReport *eventbenchmark.TestCaseBenchmarkReport
	TestSpec        *api.TestSpec
}

func (report *TestSuiteReport) NumSuccesses() int {
	if report == nil {
		return 0
	}
	rv := 0
	for _, r := range report.TestCaseReports {
		if r != nil && r.FailureReason == "" {
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

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	g, ctx := errgroup.WithContext(ctx)
	eventLogger := eventlogger.New(make(chan *api.EventMessage), time.Second)
	eventLogger.Out = a.Out

	wg := sync.WaitGroup{}
	wg.Add(len(testSpecs))
	for i, testSpec := range testSpecs {
		i := i
		testRunner := TestRunner{
			Out:                  a.Out,
			apiConnectionDetails: a.Params.ApiConnectionDetails,
			testSpec:             testSpec,
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
	g.Go(func() error { return eventLogger.Run(ctx) })

	wg.Wait()
	cancel()
	if err := g.Wait(); err != nil {
		return nil, err
	}

	return rv, nil
}

// UnmarshalTestCase unmarshalls bytes into a TestSpec.
func UnmarshalTestCase(yamlBytes []byte, testSpec *api.TestSpec) error {
	var result *multierror.Error
	successExpectedEvents := false
	successEverythingElse := false
	yamlSpecSeparator := []byte("---")
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
	return nil
}
