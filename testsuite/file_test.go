package testsuite

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mattn/go-zglob"
	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/internal/testsuite"
	"github.com/G-Research/armada/pkg/client"
	"github.com/G-Research/armada/pkg/client/util"
)

const (
	testFileFailureUnschedulableAffinity     = "failure_unschedulable_affinity_1x1"
	testFileFailureUnschedulableNodeSelector = "failure_unschedulable_nodeselector_1x1"
)

func TestFiles(t *testing.T) {

	// Load in Armada config.
	armadaConfigFile := os.Getenv("ARMADA_CONFIG")
	if armadaConfigFile == "" {
		homeDir, err := os.UserHomeDir()
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		armadaConfigFile = filepath.Join(homeDir, ".armadactl.yaml")
	}
	if !assert.NotEmpty(t, armadaConfigFile, "ARMADA_CONFIG is empty") {
		t.FailNow()
	}
	apiConnectionDetails := &client.ApiConnectionDetails{}
	err := util.BindJsonOrYaml(armadaConfigFile, apiConnectionDetails)
	if !assert.NoErrorf(t, err, "error unmarshalling api connection details") {
		t.FailNow()
	}

	healthy, err := apiConnectionDetails.ArmadaHealthCheck()
	if !assert.NoErrorf(t, err, "error performing Armada health check") {
		t.FailNow()
	}
	if !assert.Truef(t, healthy, "Armada server is unhealthy") {
		t.FailNow()
	}

	testSuite := testsuite.New()
	testSuite.Params.ApiConnectionDetails = apiConnectionDetails

	// Load test files.
	testFilesPattern := os.Getenv("ARMADA_TEST_FILES")
	if testFilesPattern == "" {
		testFilesPattern = "testcases/basic/*.yaml"
	}
	if !assert.NotEmpty(t, testFilesPattern, "no test cases provided") {
		t.FailNow()
	}

	testFiles, err := zglob.Glob(testFilesPattern)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	for _, testFile := range testFiles {
		name := filepath.Base(testFile)
		ext := filepath.Ext(name)
		if !assert.NotEmpty(t, ext) {
			continue
		}
		name = strings.TrimSuffix(name, ext)
		switch name {
		case testFileFailureUnschedulableAffinity:
			testFailureUnschedulableAffinity(t, testSuite, testFile)
		case testFileFailureUnschedulableNodeSelector:
			testFailureUnschedulableNodeSelector(t, testSuite, testFile)
		default:
			t.Run(name, func(t *testing.T) {
				assert.NoError(t, testSuite.TestFile(context.Background(), testFile))
			})
		}
	}
}

func testFailureUnschedulableAffinity(t *testing.T, testSuite *testsuite.App, testFile string) {
	t.Helper()

	t.Run(testFileFailureUnschedulableAffinity, func(t *testing.T) {
		err := testSuite.TestFile(context.Background(), testFile)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "node affinity does not match any node selector terms")
	})
}

func testFailureUnschedulableNodeSelector(t *testing.T, testSuite *testsuite.App, testFile string) {
	t.Helper()

	t.Run(testFileFailureUnschedulableNodeSelector, func(t *testing.T) {
		err := testSuite.TestFile(context.Background(), testFile)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "node selector requires labels")
	})
}
