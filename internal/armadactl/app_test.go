package armadactl

import (
	"bytes"
	"context"
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
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"google.golang.org/grpc"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/pkg/api"
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
		ArmadaUrl: "localhost:50052",
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
		ArmadaUrl: "localhost:50052",
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
		ArmadaUrl: "localhost:50052",
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

	// create a queue to use for the tests
	err = app.CreateQueue(name, priorityFactor, owners, groups, resourceLimits)
	if err != nil {
		t.Fatalf("error creating test queue: %s", err)
	}
	buf.Reset()

	t.Run("submit", func(t *testing.T) {
		err := app.Submit(path, false)
		if err != nil {
			t.Fatalf("expected no error, but got %s", err)
		}

		out := buf.String()
		buf.Reset()
		for _, s := range []string{"Submitted job with ID", "to job set with ID set1\n"} {
			if !strings.Contains(out, s) {
				t.Fatalf("expected output to contain '%s', but got '%s'", s, out)
			}
		}
	})

	t.Run("resources", func(t *testing.T) {
		err := app.Resources(name, "set1")
		if err != nil {
			t.Fatalf("expected no error, but got %s", err)
		}

		out := buf.String()
		buf.Reset()
		for _, s := range []string{"Job ID:", "maximum used resources:", "\n"} {
			if !strings.Contains(out, s) {
				t.Fatalf("expected output to contain '%s', but got '%s'", s, out)
			}
		}
	})

	t.Run("analyze", func(t *testing.T) {
		err := app.Analyze(name, "set1") // set1 is hard-coded in the jobfile
		if err != nil {
			t.Fatalf("expected no error, but got %s", err)
		}

		out := buf.String()
		buf.Reset()
		for _, s := range []string{fmt.Sprintf("Querying queue %s for job set set1", name), "api.JobSubmittedEvent", "api.JobQueuedEvent"} {
			if !strings.Contains(out, s) {
				t.Fatalf("expected output to contain '%s', but got '%s'", s, out)
			}
		}
	})

	t.Run("reprioritize", func(t *testing.T) {
		err := app.Reprioritize("", name, "set1", 0)
		if err != nil {
			t.Fatalf("expected no error, but got %s", err)
		}

		out := buf.String()
		buf.Reset()
		for _, s := range []string{"Reprioritized jobs with ID:\n"} {
			if !strings.Contains(out, s) {
				t.Fatalf("expected output to contain '%s', but got '%s'", s, out)
			}
		}
	})

	// TODO watch runs forever, making it impossible to test
	// t.Run("watch", func(t *testing.T) {
	// 	raw := false
	// 	exit_on_inactive := true
	// 	err := app.Watch(name, "set1", raw, exit_on_inactive)
	// 	if err != nil {
	// 		t.Fatalf("expected no error, but got %s", err)
	// 	}

	// 	out := buf.String()
	// 	buf.Reset()
	// 	fmt.Println("watch out:", out)
	// 	for _, s := range []string{"Reprioritized jobs with ID:\n"} {
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

	// define the cleanup function here so it can be called to clean up containers in the case of partial success
	var redisCleanup, postgresCleanup, jetstreamCleanup, armadaServerCleanup func()
	cleanup := func() {
		if armadaServerCleanup != nil {
			armadaServerCleanup()
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

	// wait for the Armada server to come up
	// pool.MaxWait = 5 * time.Minute // max time until pool.Retry returns an error
	if err = pool.Retry(func() error {
		_, err := http.Get("http://localhost:8081/health") // Armada server HTTP REST API endpoint; should return code 204
		if err != nil {
			return fmt.Errorf("[spinUpArmadaCluster] error waiting for Armada server to start: %s", err)
		}
		return nil
	}); err != nil {
		cleanup()
		return nil, err
	}

	// make API calls to the server necessary for it to accept jobs
	setupServer()

	return cleanup, nil
}

// setupServer makes API calls to the server to tell it there are resources available
func setupServer() error {

	conn, err := grpc.Dial("localhost:50052", grpc.WithInsecure(), grpc.WithDefaultCallOptions(grpc.WaitForReady(true)))
	if err != nil {
		return fmt.Errorf("[setupServer] error making gRPC call to server: %s", err)
	}
	defer conn.Close()

	ctx := context.Background()
	usageReport := &api.ClusterUsageReport{
		ClusterId:                "test-cluster",
		ReportTime:               time.Now(),
		Queues:                   []*api.QueueReport{},
		ClusterCapacity:          map[string]resource.Quantity{"cpu": resource.MustParse("100"), "memory": resource.MustParse("100Gi")},
		ClusterAvailableCapacity: map[string]resource.Quantity{"cpu": resource.MustParse("100"), "memory": resource.MustParse("100Gi")},
	}
	usageClient := api.NewUsageClient(conn)
	_, err = usageClient.ReportUsage(ctx, usageReport)
	if err != nil {
		return fmt.Errorf("[setupServer] error reporting usage to server: %s", err)
	}

	// make initial lease request to populate cluster node info
	leaseClient := api.NewAggregatedQueueClient(conn)
	_, err = leaseJobs(leaseClient, ctx)
	if err != nil {
		return fmt.Errorf("[setupServer] error making lease request: %s", err)
	}

	return nil
}

func leaseJobs(leaseClient api.AggregatedQueueClient, ctx context.Context) (*api.JobLease, error) {
	nodeResources := common.ComputeResources{"cpu": resource.MustParse("5"), "memory": resource.MustParse("5Gi")}
	return leaseClient.LeaseJobs(ctx, &api.LeaseRequest{
		ClusterId: "test-cluster",
		Resources: nodeResources,
		Nodes:     []api.NodeInfo{{Name: "testNode", AllocatableResources: nodeResources, AvailableResources: nodeResources}},
	})
}

// spinUpRedis runs redis in a container with hard-coded values and returns a cleanup function handle
func spinUpRedis(pool *dockertest.Pool, network *docker.Network) (func(), error) {
	opts := &dockertest.RunOptions{
		Repository: "redis",
		Tag:        "6.2",
		Name:       "armada-test-redis",
		Hostname:   "redis",
		NetworkID:  network.ID,
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
		Repository: "postgres",
		Tag:        "10",
		Name:       "armada-test-postgres",
		Hostname:   "postgres",
		NetworkID:  network.ID,
		Env:        []string{"POSTGRES_PASSWORD=psw postgres"},
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
		Cmd:        []string{"-c", "/app/jetstream.conf"},
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
		Mounts:       []string{rootDir + ":/app"},                   // mount the project root directory into the container
		ExposedPorts: []string{"50051/tcp", "8080/tcp", "9000/tcp"}, // need both ExposedPorts and PortBindings
		PortBindings: map[docker.Port][]docker.PortBinding{
			"50051/tcp": []docker.PortBinding{{HostPort: "50052"}}, // gRPC
			"8080/tcp":  []docker.PortBinding{{HostPort: "8081"}},  // HTTP
			"9000/tcp":  []docker.PortBinding{{HostPort: "9002"}},  // metrics
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
