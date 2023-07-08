package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

var Gotestsum string

var LocalBin = filepath.Join(os.Getenv("PWD"), "/bin")

func makeLocalBin() error {
	if _, err := os.Stat(LocalBin); os.IsNotExist(err) {
		err = os.MkdirAll(LocalBin, os.ModePerm)
		if err != nil {
			return err
		}
	}
	return nil
}

// Gotestsum downloads gotestsum locally if necessary
func gotestsum() error {
	mg.Deps(makeLocalBin)
	Gotestsum = filepath.Join(LocalBin, "/gotestsum")

	if _, err := os.Stat(Gotestsum); os.IsNotExist(err) {
		fmt.Println(Gotestsum)
		cmd := exec.Command("go", "install", "gotest.tools/gotestsum@v1.8.2")
		cmd.Env = append(os.Environ(), "GOBIN="+LocalBin)
		return cmd.Run()

	}
	return nil
}

// Tests is a mage target that runs the tests and generates coverage reports.
func Tests() error {
	mg.Deps(gotestsum)
	var err error

	docker_Net, err := dockerNet()
	if err != nil {
		return err
	}

	err = dockerRun("run", "-d", "--name=redis", docker_Net, "-p=6379:6379", "redis:6.2.6")
	if err != nil {
		return err
	}

	err = dockerRun("run", "-d", "--name=postgres", docker_Net, "-p", "5432:5432", "-e", "POSTGRES_PASSWORD=psw", "postgres:14.2")
	if err != nil {
		return err
	}

	err = sh.Run("sleep", "3")
	if err != nil {
		return err
	}
	packages, err := sh.Output("go", "list", "./internal/...")
	if err != nil {
		return err
	}

	internalPackages := filterPackages(strings.Fields(packages), "jobservice/repository")

	err = runtest("internal_coverage.xml", "internal.txt", true, internalPackages...)
	if err != nil {
		return err
	}

	os.Setenv("JSDBTYPE", "sqlite")
	err = runtest("", "internal.txt", true, "./internal/jobservice/repository/...")
	if err != nil {
		return err
	}

	os.Setenv("JSDBTYPE", "postgres")
	err = runtest("", "internal.txt", true, "./internal/jobservice/repository/...")
	if err != nil {
		return err
	}
	os.Unsetenv("JSDBTYPE")
	err = runtest("pkg_coverage.xml", "pkg.txt", false, "./pkg...")
	if err != nil {
		return err
	}

	err = runtest("cmd_coverage.xml", "cmd.txt", false, "./cmd...")
	if err != nil {
		return err
	}

	defer func() {
		dockerErr := dockerRun("rm", "-f", "redis", "postgres")
		if dockerErr != nil {
			if err == nil {
				err = dockerErr
			} else {
				err = fmt.Errorf("%w; %s", err, dockerErr.Error())
			}
		}
	}()

	return err
}

func runtest(coverageFileName, outputFileName string, appendOutput bool, directories ...string) error {
	args := []string{"--", "-v"}
	if coverageFileName != "" {
		args = append(args, "-coverprofile", coverageFileName)
	}
	args = append(args, directories...)

	cmd := exec.Command(Gotestsum, args...)

	fileFlags := os.O_WRONLY | os.O_CREATE
	if appendOutput {
		fileFlags |= os.O_APPEND
	} else {
		fileFlags |= os.O_TRUNC
	}

	file, err := os.OpenFile(filepath.Join("test_reports", outputFileName), fileFlags, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()

	cmd.Stdout = io.MultiWriter(os.Stdout, file)
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func filterPackages(packages []string, filter string) []string {
	var filtered []string
	for _, pkg := range packages {
		if !strings.Contains(pkg, filter) {
			filtered = append(filtered, pkg)
		}
	}
	return filtered
}

func runTest(name, outputFileName string) error {
	cmd := exec.Command(Gotestsum, "--", "-v", name, "-count=1")
	file, err := os.Create(filepath.Join("test_reports", outputFileName))
	if err != nil {
		return err
	}
	defer file.Close()
	cmd.Stdout = io.MultiWriter(os.Stdout, file)
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// Testse2eENoSetup runs the E2E tests without setup
func Testse2eNoSetup() error {
	mg.Deps(gotestsum)
	printApplicationLogs := func() {
		fmt.Println("\nexecutor logs:")
		output, err := dockerOutput("logs", "executor")
		fmt.Println(output)
		if err != nil {
			fmt.Println(err)
		}

		fmt.Println("\nserver logs:")
		output, err = dockerOutput("logs", "server")
		fmt.Println(output)
		if err != nil {
			fmt.Println(err)
		}
	}

	defer printApplicationLogs()

	if err := os.MkdirAll("test_reports", os.ModePerm); err != nil {
		return err
	}

	if err := runTest("./e2e/armadactl_test/...", "e2e_armadactl.txt"); err != nil {
		return err
	}
	if err := runTest("./e2e/pulsar_test/...", "e2e_pulsar.txt"); err != nil {
		return err
	}
	if err := runTest("./e2e/pulsartest_client/...", "e2e_pulsartest_client.txt"); err != nil {
		return err
	}
	if err := runTest("./e2e/lookout_ingester_test/...", "e2e_lookout_ingester.txt"); err != nil {
		return err
	}

	cmd := dotnetCmd()
	cmd = append(cmd, "dotnet", "test", "client/DotNet/Armada.Client.Test/Armada.Client.Test.csproj")
	if err := dockerRun(cmd...); err != nil {
		return err
	}

	return nil
}

// Teste2eSetup runs the test with local cluster
func Teste2eSetup() error {
	if err := BuildDockers("server,lookoutingester,executor,eventingester,jobservice"); err != nil {
		return err
	}
	mg.Deps(Kind)
	mg.Deps(StartDependencies)
	os.Setenv("ARMADA_COMPONENTS", "server,lookoutingester,executor,eventingester,jobservice")
	mg.Deps(StartComponents)

	if err := dockerRun("run", "-d", "--name", "lookout-ingester-migrate", "--network=kind", "-v", "${PWD}/e2e:/e2e",
		"gresearch/lookoutingester", "--config", "/e2e/setup/lookout-ingester-config.yaml", "--migrateDatabase"); err != nil {
		return err
	}

	go_cmd, err := go_CMD()
	if err != nil {
		return err
	}
	createQueue := func(queueName string) error {
		cmd := append(go_cmd, "run", "cmd/armadactl/main.go", "create", "queue", queueName)
		if err = dockerRun(cmd...); err != nil {
			return err
		}
		return nil
	}
	if err = createQueue("e2e-test-queue"); err != nil {
		fmt.Println(err)
	}
	if err = createQueue("queue-a"); err != nil {
		fmt.Println(err)
	}
	if err = createQueue("queue-b"); err != nil {
		fmt.Println(err)
	}

	mg.Deps(gotestsum)

	if err := os.MkdirAll("test_reports", os.ModePerm); err != nil {
		return err
	}
	time.Sleep(10 * time.Second)

	defer func() {
		fmt.Println("\nexecutor logs:")
		if err := dockerRun("logs", "executor"); err != nil {
			fmt.Println("Error retrieving executor logs:", err)
		}

		fmt.Println("\nserver logs:")
		if err := dockerRun("logs", "server"); err != nil {
			fmt.Println("Error retrieving server logs:", err)
		}

		LocalDevStop()

		if err := sh.Run("rm", ".kube/config"); err != nil {
			fmt.Println("Error removing .kube/config:", err)
		}
		if err := sh.Run("rmdir", ".kube"); err != nil {
			fmt.Println("Error removing .kube directory:", err)
		}
	}()

	if err := runTest("./e2e/armadactl_test/...", "e2e_armadactl.txt"); err != nil {
		return err
	}
	if err := runTest("./e2e/basic_test/...", "e2e_basic.txt"); err != nil {
		return err
	}
	if err := runTest("./e2e/pulsar_test/...", "e2e_pulsar.txt"); err != nil {
		return err
	}
	if err := runTest("./e2e/pulsartest_client/...", "e2e_pulsartest_client.txt"); err != nil {
		return err
	}
	if err := runTest("./e2e/lookout_ingester_test/...", "e2e_lookout_ingester.txt"); err != nil {
		return err
	}
	cmd := dotnetCmd()
	cmd = append(cmd, "dotnet", "test", "client/DotNet/Armada.Client.Test/Armada.Client.Test.csproj")
	if err := dockerRun(cmd...); err != nil {
		return err
	}

	return nil
}

// Teste2eAirflow runs e2e tests for airflow
func Teste2eAirflow() error {
	mg.Deps(AirflowOperator)
	if err := BuildDockers("jobservice"); err != nil {
		return err
	}

	cmd, err := go_CMD()
	if err != nil {
		return err
	}
	cmd = append(cmd, "go", "run", "cmd/armadactl/main.go", "create", "queue", "queue-a")
	if err := dockerRun(cmd...); err != nil {
		fmt.Println(err)
	}

	if err := dockerRun("rm", "-f", "jobservice"); err != nil {
		fmt.Println(err)
	}

	output, err := dockerOutput("run", "-d", "--name", "jobservice", "--network=kind",
		"--mount", "type=bind,src=${PWD}/e2e,dst=/e2e", "gresearch/armada-jobservice", "run", "--config",
		"/e2e/setup/jobservice.yaml")
	fmt.Println(output)
	if err != nil {
		return err
	}

	output, err = dockerOutput("run", "-v", "${PWD}/e2e:/e2e", "-v", "${PWD}/third_party/airflow:/code",
		"--workdir", "/code", "-e", "ARMADA_SERVER=server", "-e", "ARMADA_PORT=50051", "-e", "JOB_SERVICE_HOST=jobservice",
		"-e", "JOB_SERVICE_PORT=60003", "--entrypoint", "python3", "--network=kind", "armada-airflow-operator-builder:latest",
		"-m", "pytest", "-v", "-s", "/code/tests/integration/test_airflow_operator_logic.py")
	fmt.Println(output)
	if err != nil {
		fmt.Println(err)
	}

	err = dockerRun("rm", "-f", "jobservice")
	if err != nil {
		fmt.Println(err)
	}
	return nil
}

// Teste2epython runs e2e tests for python client
func Teste2epython() error {
	mg.Deps(BuildPython)
	args := []string{
		"run",
		"-v", "${PWD}/client/python:/code",
		"--workdir", "/code",
		"-e", "ARMADA_SERVER=server",
		"-e", "ARMADA_PORT=50051",
		"--entrypoint", "python3",
		"--network", "kind",
		"armada-python-client-builder:latest",
		"-m", "pytest",
		"-v", "-s",
		"/code/tests/integration/test_no_auth.py",
	}

	return dockerRun(args...)
}

// TestsNoSetup runs the tests without setup
func TestsNoSetup() error {
	mg.Deps(gotestsum)

	if err := runTest("./internal...", "internal.txt"); err != nil {
		return err
	}
	if err := runTest("./pkg...", "pkg.txt"); err != nil {
		return err
	}
	if err := runTest("./cmd...", "cmd.txt"); err != nil {
		return err
	}

	return nil
}

// PopulateLookoutTest populates the lookout test
func PopulateLookoutTest() error {
	dockerNet, err := dockerNet()
	if err != nil {
		return err
	}
	if err = dockerRun("ps", "-q", "-f", "name=postgres"); err == nil {

		if err := dockerRun("stop", "postgres"); err != nil {
			return err
		}
		if err := dockerRun("rm", "postgres"); err != nil {
			return err
		}
	}

	err = dockerRun("run", "-d", "--name=postgres", dockerNet, "-p", "5432:5432", "-e", "POSTGRES_PASSWORD=psw", "postgres:14.2")
	if err != nil {
		return err
	}

	time.Sleep(5 * time.Second)

	err = goRun("test", "-v", "${PWD}/internal/lookout/db-gen/")
	if err != nil {
		return err
	}

	return nil
}
