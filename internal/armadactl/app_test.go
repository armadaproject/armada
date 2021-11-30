package armadactl

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"

	"github.com/G-Research/armada/pkg/client"
	cq "github.com/G-Research/armada/pkg/client/queue"
)

func TestMain(m *testing.M) {
	exitCode, err := runTests(m)
	if err != nil {
		log.Printf("error setting up or tearing down test environment: %s", err)
	}
	os.Exit(exitCode)
}

func runTests(m *testing.M) (int, error) {
	cleanup, err := spinUpArmadaCluster()
	if err != nil {
		return -1, fmt.Errorf("[runTests] error spinning up Armada cluster: %s", err)
	}
	defer cleanup()
	return m.Run(), nil
}

func TestVersion(t *testing.T) {
	buf := new(bytes.Buffer)
	app := &App{
		Params: &Params{},
		Out:    buf,
		Random: rand.Reader,
	}
	app.Params.QueueAPI = &QueueAPI{}
	app.Params.ApiConnectionDetails = &client.ApiConnectionDetails{
		ArmadaUrl: "localhost:50051",
	}

	err := app.Version()
	if err != nil {
		t.Fatalf("expected no error, but got %s", err)
	}

	out := buf.String()
	for _, s := range []string{"Commit", "Go version", "Built"} {
		if !strings.Contains(out, s) {
			t.Fatalf("expected output to contain %s, but got %s", s, out)
		}
	}
}

func TestQueue(t *testing.T) {
	buf := new(bytes.Buffer)
	app := &App{
		Params: &Params{},
		Out:    buf,
		Random: rand.Reader,
	}
	app.Params.QueueAPI = &QueueAPI{}
	app.Params.ApiConnectionDetails = &client.ApiConnectionDetails{
		ArmadaUrl: "localhost:50051",
	}

	// Setup the armadactl to use pkg/client as its backend for queue-related commands
	f := func() *client.ApiConnectionDetails {
		return app.Params.ApiConnectionDetails
	}
	app.Params.QueueAPI.Create = cq.Create(f)
	app.Params.QueueAPI.Delete = cq.Delete(f)
	app.Params.QueueAPI.GetInfo = cq.GetInfo(f)
	app.Params.QueueAPI.Update = cq.Update(f)

	// queue parameters
	name := "foo"
	priorityFactor := 1.337
	owners := []string{"ubar", "ubaz"}
	groups := []string{"gbar", "gbaz"}
	resourceLimits := map[string]float64{"cpu": 0.2, "exoticResource": 0.9}

	t.Run("create", func(t *testing.T) {
		err := app.CreateQueue(name, priorityFactor, owners, groups, resourceLimits)
		if err != nil {
			t.Fatalf("expected no error, but got %s", err)
		}

		out := buf.String()
		buf.Reset()
		for _, s := range []string{fmt.Sprintf("Created queue %s\n", name)} {
			if !strings.Contains(out, s) {
				t.Fatalf("expected output to contain '%s', but got '%s'", s, out)
			}
		}
	})

	t.Run("describe", func(t *testing.T) {
		err := app.DescribeQueue(name)
		if err != nil {
			t.Fatalf("expected no error, but got %s", err)
		}

		out := buf.String()
		buf.Reset()
		for _, s := range []string{fmt.Sprintf("Queue: %s\n", name), "No queued or running jobs\n"} {
			if !strings.Contains(out, s) {
				t.Fatalf("expected output to contain '%s', but got '%s'", s, out)
			}
		}
	})

	t.Run("change", func(t *testing.T) {
		err := app.UpdateQueue(name, priorityFactor, owners, groups, resourceLimits)
		if err != nil {
			t.Fatalf("expected no error, but got %s", err)
		}

		out := buf.String()
		buf.Reset()
		for _, s := range []string{fmt.Sprintf("Updated queue %s\n", name)} {
			if !strings.Contains(out, s) {
				t.Fatalf("expected output to contain '%s', but got '%s'", s, out)
			}
		}
	})

	t.Run("delete", func(t *testing.T) {
		err := app.DeleteQueue(name)
		if err != nil {
			t.Fatalf("expected no error, but got %s", err)
		}

		out := buf.String()
		buf.Reset()
		for _, s := range []string{"Deleted", name, "\n"} {
			if !strings.Contains(out, s) {
				t.Fatalf("expected output to contain '%s', but got '%s'", s, out)
			}
		}
	})

	// TODO armadactl returns empty output for non-existing queues
	// // request details about the queue
	// err = app.DescribeQueue(name)
	// if err == nil {
	// 	t.Fatal("expected an error, but got none")
	// }

	// // change queue details
	// err = app.UpdateQueue(name, priorityFactor, owners, groups, resourceLimits)
	// if err == nil {
	// 	t.Fatal("expected an error, but got none")
	// }
}

func TestJob(t *testing.T) {
	buf := new(bytes.Buffer)
	app := &App{
		Params: &Params{},
		Out:    buf,
		Random: rand.Reader,
	}
	app.Params.QueueAPI = &QueueAPI{}
	app.Params.ApiConnectionDetails = &client.ApiConnectionDetails{
		ArmadaUrl: "localhost:50051",
	}

	// Setup the armadactl to use pkg/client as its backend for queue-related commands
	f := func() *client.ApiConnectionDetails {
		return app.Params.ApiConnectionDetails
	}
	app.Params.QueueAPI.Create = cq.Create(f)
	app.Params.QueueAPI.Delete = cq.Delete(f)
	app.Params.QueueAPI.GetInfo = cq.GetInfo(f)
	app.Params.QueueAPI.Update = cq.Update(f)

	// queue parameters
	name := "test"
	priorityFactor := 1.337
	owners := []string{"ubar", "ubaz"}
	groups := []string{"gbar", "gbaz"}
	resourceLimits := map[string]float64{"cpu": 0.2, "exoticResource": 0.9}

	// job parameters
	path, err := filepath.Abs(filepath.Join("./", "app_test_job.yaml"))
	if err != nil {
		t.Fatalf("error creating path to test job: %s", err)
	}
	// dryRun := false
	fmt.Println("jobfile:", path)

	// create a queue to use for the tests
	err = app.CreateQueue(name, priorityFactor, owners, groups, resourceLimits)
	if err != nil {
		t.Fatalf("error creating test queue: %s", err)
	}
	buf.Reset()

	// TODO there are TSL-related problems with running the fakeexecutor in a container,
	// and without an executor it's not possible to submit jobs

	// t.Run("submit", func(t *testing.T) {
	// 	err := app.Submit(path, false)
	// 	if err != nil {
	// 		t.Fatalf("expected no error, but got %s", err)
	// 	}

	// 	out := buf.String()
	// 	fmt.Println("submit out:", out)
	// 	buf.Reset()
	// 	for _, s := range []string{"Submitted job with ID", "to job set with ID set1\n"} {
	// 		if !strings.Contains(out, s) {
	// 			t.Fatalf("expected output to contain '%s', but got '%s'", s, out)
	// 		}
	// 	}
	// })

	// t.Run("resources", func(t *testing.T) {
	// 	err := app.Resources(name, "set1")
	// 	if err != nil {
	// 		t.Fatalf("expected no error, but got %s", err)
	// 	}

	// 	out := buf.String()
	// 	buf.Reset()
	// 	for _, s := range []string{"Job ID:", "maximum used resources: 0\n"} {
	// 		if !strings.Contains(out, s) {
	// 			t.Fatalf("expected output to contain '%s', but got '%s'", s, out)
	// 		}
	// 	}
	// })

	t.Run("cancel", func(t *testing.T) {
		err := app.Cancel(name, "set1", "")
		if err != nil {
			t.Fatalf("expected no error, but got %s", err)
		}

		out := buf.String()
		buf.Reset()
		for _, s := range []string{"Requested cancellation for jobs", "\n"} {
			if !strings.Contains(out, s) {
				t.Fatalf("expected output to contain '%s', but got '%s'", s, out)
			}
		}
	})
}

// spinUpArmadaCluster spins up a containerized Armada cluster returns a cleanup function handle
func spinUpArmadaCluster() (func(), error) {

	// defaults to tcp/http on windows and socket on linux/osx when called with ""
	pool, err := dockertest.NewPool("")
	if err != nil {
		return nil, fmt.Errorf("[spinUpArmadaCluster] error connecting to docker: %s", err)
	}
	if err = pool.Client.Ping(); err != nil {
		return nil, fmt.Errorf("[spinUpArmadaCluster] could not connect to docker: %s", err)
	}

	// create a new network for the cluster
	network, err := pool.Client.CreateNetwork(docker.CreateNetworkOptions{Name: "armada-test-network"})
	if err != nil {
		return nil, fmt.Errorf("[spinUpArmadaCluster] error creating test network: %s", err)
	}

	// define the cleanup function here so it can be called to clean up dockers in the case of partial success
	var redisCleanup, postgresCleanup, jetstreamCleanup, executorCleanup, armadaServerCleanup func()
	cleanup := func() {
		if armadaServerCleanup != nil {
			armadaServerCleanup()
		}
		if executorCleanup != nil {
			executorCleanup()
		}
		if jetstreamCleanup != nil {
			jetstreamCleanup()
		}
		if postgresCleanup != nil {
			postgresCleanup()
		}
		if redisCleanup != nil {
			redisCleanup()
		}
		if err = pool.Client.RemoveNetwork(network.ID); err != nil {
			log.Printf("[spinUpArmadaCluster] error removing network %s: %s", network.Name, err)
		}
	}

	// start all required services
	redisCleanup, err = spinUpRedis(pool, network)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("[spinUpArmadaCluster] error starting redis: %s", err)
	}

	postgresCleanup, err = spinUpPostgres(pool, network)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("[spinUpArmadaCluster] error starting postgres: %s", err)
	}

	jetstreamCleanup, err = spinUpJetstream(pool, network)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("[spinUpArmadaCluster] error starting jetstream: %s", err)
	}

	armadaServerCleanup, err = spinUpArmadaServer(pool, network)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("[spinUpArmadaCluster] error starting Armada server: %s", err)
	}

	executorCleanup, err = spinUpFakeExecutor(pool, network)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("[spinUpArmadaCluster] error starting fake executor: %s", err)
	}

	// wait for the Armada server to come up
	// pool.MaxWait = 5 * time.Minute // max time until pool.Retry returns an error
	if err = pool.Retry(func() error {
		_, err := http.Get("http://localhost:8080/health") // Armada server HTTP REST API endpoint; should return code 204
		if err != nil {
			return fmt.Errorf("[spinUpArmadaCluster] error waiting for Armada server to start: %s", err)
		}
		return nil
	}); err != nil {
		cleanup()
		return nil, err
	}

	// wait for the Armada executor to come up
	// pool.MaxWait = 5 * time.Minute // max time until pool.Retry returns an error
	if err = pool.Retry(func() error {
		_, err := http.Get("http://localhost:9001/") // Should return 404 once the executor is up
		if err != nil {
			return fmt.Errorf("[spinUpArmadaCluster] error waiting for Armada executor to start: %s", err)
		}
		return nil
	}); err != nil {
		cleanup()
		return nil, err
	}

	return cleanup, nil
}

// spinUpRedis runs redis in a container with hard-coded values and returns a cleanup function handle
func spinUpRedis(pool *dockertest.Pool, network *docker.Network) (func(), error) {
	opts := &dockertest.RunOptions{
		Repository:   "redis",
		Tag:          "6.2",
		Name:         "armada-test-redis",
		Hostname:     "redis",
		NetworkID:    network.ID,
		PortBindings: map[docker.Port][]docker.PortBinding{"6379/tcp": []docker.PortBinding{{HostPort: "6379"}}},
	}
	cleanup, err := spinUpService(opts, pool, network)
	if err != nil {
		return nil, fmt.Errorf("[spinUpRedis] error starting redis: %s", err)
	}
	return cleanup, nil
}

// spinUpPostgres runs postgres in a container with hard-coded values and returns a cleanup function handle
func spinUpPostgres(pool *dockertest.Pool, network *docker.Network) (func(), error) {
	opts := &dockertest.RunOptions{
		Repository:   "postgres",
		Tag:          "10",
		Name:         "armada-test-postgres",
		Hostname:     "postgres",
		NetworkID:    network.ID,
		PortBindings: map[docker.Port][]docker.PortBinding{"5432/tcp": []docker.PortBinding{{HostPort: "5432"}}},
		Env:          []string{"POSTGRES_PASSWORD=psw postgres"},
	}
	cleanup, err := spinUpService(opts, pool, network)
	if err != nil {
		return nil, fmt.Errorf("[spinUpPostgres] error starting postgres: %s", err)
	}
	return cleanup, nil
}

// spinUpJetstream runs jetstream in a container with hard-coded values and returns a cleanup function handle
func spinUpJetstream(pool *dockertest.Pool, network *docker.Network) (func(), error) {
	jetstreamConfigDir, err := filepath.Abs("../.././docs/dev/config/jetstream/") // assumed to contain jetstream.conf
	if err != nil {
		return nil, fmt.Errorf("[spinUpJetstream] error creating path to jetstream config: %s", err)
	}
	opts := &dockertest.RunOptions{
		Repository: "nats",
		Tag:        "2.6.1-alpine",
		Name:       "armada-test-nats",
		Hostname:   "nats",
		NetworkID:  network.ID,
		Mounts:     []string{jetstreamConfigDir + ":/app"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"4222/tcp": []docker.PortBinding{{HostPort: "4222"}},
			"6222/tcp": []docker.PortBinding{{HostPort: "6222"}},
			"8222/tcp": []docker.PortBinding{{HostPort: "8222"}},
		},
		Cmd: []string{"-c", "/app/jetstream.conf"},
	}
	cleanup, err := spinUpService(opts, pool, network)
	if err != nil {
		return nil, fmt.Errorf("[spinUpJetstream] error starting jetstream: %s", err)
	}
	return cleanup, nil
}

func spinUpArmadaServer(pool *dockertest.Pool, network *docker.Network) (func(), error) {

	// compile the Armada server on the host and mount the resulting binary into the container
	// compiling on the host is much faster than in a container since deps. are cached between runs
	rootDir, err := filepath.Abs("../../")
	if err != nil {
		return nil, fmt.Errorf("[spinUpArmadaServer] error creating path to root directory: %s", err)
	}
	err = compile(filepath.Join(rootDir, "/.test/", "server"), filepath.Join(rootDir, "/cmd/", "/armada/", "main.go"))
	if err != nil {
		return nil, fmt.Errorf("[spinUpArmadaServer] error compiling Armada server: %s", err)
	}

	// container opts.
	opts := &dockertest.RunOptions{
		Repository:   "golang",
		Tag:          "1.17.3",
		Name:         "armada-test-server",
		Hostname:     "armada",
		NetworkID:    network.ID,
		Mounts:       []string{rootDir + ":/app"},                   // mount the compiled binary into the container
		ExposedPorts: []string{"50051/tcp", "8080/tcp", "9000/tcp"}, // need both ExposedPorts and PortBindings
		PortBindings: map[docker.Port][]docker.PortBinding{
			"50051/tcp": []docker.PortBinding{{HostPort: "50051"}}, // gRPC
			"8080/tcp":  []docker.PortBinding{{HostPort: "8080"}},  // HTTP
			"9000/tcp":  []docker.PortBinding{{HostPort: "9000"}},  // metrics
		},
		WorkingDir: "/app",
		Entrypoint: []string{"./.test/server"},
		Cmd: []string{
			"--config", "./docs/dev/config/armada/auth.yaml",
			"--config", "./docs/dev/config/armada/jetstream.yaml",
			"--config", "./internal/armadactl/app_test_armada_config.yaml",
		},
	}

	cleanup, err := spinUpService(opts, pool, network)
	if err != nil {
		return nil, fmt.Errorf("[spinUpArmadaServer] error starting Armada server: %s", err)
	}
	return cleanup, nil
}

func spinUpFakeExecutor(pool *dockertest.Pool, network *docker.Network) (func(), error) {

	// compile the fake executor on the host and mount the resulting binary into the container
	// compiling on the host is much faster than in a container since deps. are cached between runs
	rootDir, err := filepath.Abs("../../")
	if err != nil {
		return nil, fmt.Errorf("[spinUpFakeExecutor] error creating path to root directory: %s", err)
	}
	err = compile(filepath.Join(rootDir, "/.test/", "fakeexecutor"), filepath.Join(rootDir, "/cmd/", "/fakeexecutor/", "main.go"))
	if err != nil {
		return nil, fmt.Errorf("[spinUpFakeExecutor] error compiling fake executor: %s", err)
	}

	// container opts.
	opts := &dockertest.RunOptions{
		Repository:   "golang",
		Tag:          "1.17.3",
		Name:         "armada-test-executor",
		Hostname:     "executor",
		NetworkID:    network.ID,
		Mounts:       []string{rootDir + ":/app"}, // mount the compiled binary into the container
		ExposedPorts: []string{"9001/tcp"},        // need both ExposedPorts and PortBindings
		PortBindings: map[docker.Port][]docker.PortBinding{
			"9001/tcp": []docker.PortBinding{{HostPort: "9001"}}, // metrics
		},
		WorkingDir: "/app",
		Entrypoint: []string{"./.test/fakeexecutor"},
		Cmd: []string{
			"--config", "./internal/armadactl/app_test_executor_config.yaml",
		},
	}

	cleanup, err := spinUpService(opts, pool, network)
	if err != nil {
		return nil, fmt.Errorf("[spinUpFakeExecutor] error starting fake executor: %s", err)
	}
	return cleanup, nil
}

// spinUpService starts a docker container based on the provided opts and returns a cleanup function handle
func spinUpService(opts *dockertest.RunOptions, pool *dockertest.Pool, network *docker.Network) (func(), error) {
	resource, err := pool.RunWithOptions(opts)
	if err != nil {
		if resource != nil { // container may have been created successfully but failed to start
			if err := pool.Purge(resource); err != nil {
				fmt.Printf("[spinUpService] error purging resource %#v; you may need to clean it up manually: %s", resource, err)
			}
		}
		return nil, fmt.Errorf("[spinUpService] error starting service: %s", err)
	}
	cleanup := func() {
		if resource != nil {
			if err := pool.Purge(resource); err != nil {
				fmt.Printf("[spinUpService] error purging resource %#v; you may need to clean it up manually: %s", resource, err)
			}
		}
	}
	return cleanup, nil
}

// compile runs 'go build -o outputPath sourcePath'
// the output directory is created if it doesn't already exist
func compile(outputPath string, sourcePath string) error {

	// create output directory
	outputDir := filepath.Dir(outputPath)
	err := os.MkdirAll(outputDir, 0777)
	if err != nil {
		return fmt.Errorf("[compile] error creating directory %s: %s", outputDir, err)
	}

	// run the compiler via exec.Command
	goBuildCmd := exec.Command("go", "build", "-o", outputPath, sourcePath)
	goBuildCmd.Env = os.Environ()
	goBuildCmd.Env = append(goBuildCmd.Env, "GOOS=linux")                             // cross-compile on Windows hosts
	goBuildCmd.Env = append(goBuildCmd.Env, fmt.Sprintf("GOARCH=%v", runtime.GOARCH)) // GOARCH equal to that of the host
	goBuildCmd.Stdout = os.Stdout
	goBuildCmd.Stderr = os.Stderr
	err = goBuildCmd.Run()
	if err != nil {
		return fmt.Errorf("[compile] error running 'go build -o %s %s': %s", outputPath, sourcePath, err)
	}

	// this call can only succeed if the output file exists
	if _, err := os.Stat(outputPath); err != nil {
		return fmt.Errorf("[compile] error stat-ing output file, it may not exist: %s", err)

	}

	return nil
}

// copyFile copies a file with path src to a file at path dst.
func copyFile(dst string, src string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("[copyFile] error opening %s: %s", src, err)
	}
	defer srcFile.Close()
	dstFile, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("[copyFile] error opening %s: %s", dst, err)
	}
	defer dstFile.Close()
	_, err = dstFile.ReadFrom(srcFile)
	if err != nil {
		return fmt.Errorf("[copyFile] error copying %s to %s: %s", src, dst, err)
	}
	return nil
}
