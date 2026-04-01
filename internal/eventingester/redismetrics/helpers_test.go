package redismetrics

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/constants"
)

const testRedisDB = 10

var testRedisAddr = "127.0.0.1:6379"

func TestMain(m *testing.M) {
	if addr := os.Getenv("ARMADA_TEST_REDIS_ADDR"); addr != "" {
		testRedisAddr = addr
		os.Exit(m.Run())
	}

	cleanup, err := startRedisContainer()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start redis test container: %v\n", err)
		os.Exit(1)
	}

	code := m.Run()
	if cleanupErr := cleanup(); cleanupErr != nil {
		fmt.Fprintf(os.Stderr, "failed to clean up redis test container: %v\n", cleanupErr)
		if code == 0 {
			code = 1
		}
	}

	os.Exit(code)
}

func startRedisContainer() (func() error, error) {
	image := os.Getenv("ARMADA_TEST_REDIS_IMAGE")
	if image == "" {
		image = "redis:7.4-alpine"
	}

	containerName := fmt.Sprintf("armada-redismetrics-test-%d", time.Now().UnixNano())
	runCmd := exec.Command("docker", "run", "-d", "-p", "127.0.0.1::6379", "--name", containerName, image)
	output, err := runCmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("docker run failed: %w: %s", err, strings.TrimSpace(string(output)))
	}

	cleanup := func() error {
		rmCmd := exec.Command("docker", "rm", "-f", containerName)
		rmOutput, rmErr := rmCmd.CombinedOutput()
		if rmErr != nil {
			return fmt.Errorf("docker rm failed: %w: %s", rmErr, strings.TrimSpace(string(rmOutput)))
		}
		return nil
	}

	portCmd := exec.Command("docker", "port", containerName, "6379/tcp")
	portOutput, err := portCmd.CombinedOutput()
	if err != nil {
		_ = cleanup()
		return nil, fmt.Errorf("docker port failed: %w: %s", err, strings.TrimSpace(string(portOutput)))
	}

	addr, err := dockerPortToAddr(strings.TrimSpace(string(portOutput)))
	if err != nil {
		_ = cleanup()
		return nil, err
	}
	testRedisAddr = addr

	if err := waitForRedis(addr, 30*time.Second); err != nil {
		_ = cleanup()
		return nil, err
	}

	return cleanup, nil
}

func dockerPortToAddr(portOutput string) (string, error) {
	parts := strings.Split(portOutput, ":")
	if len(parts) < 2 {
		return "", fmt.Errorf("unexpected docker port output: %q", portOutput)
	}

	port := strings.TrimSpace(parts[len(parts)-1])
	if port == "" {
		return "", fmt.Errorf("missing port in docker port output: %q", portOutput)
	}

	return "127.0.0.1:" + port, nil
}

func waitForRedis(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		client := redis.NewClient(&redis.Options{Addr: addr, DB: testRedisDB})
		err := client.Ping(ctx).Err()
		_ = client.Close()
		cancel()
		if err == nil {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}

	return fmt.Errorf("redis did not become ready at %s within %s", addr, timeout)
}

func withRedisClient(t *testing.T, action func(client redis.UniversalClient)) {
	t.Helper()

	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 60*time.Second)
	defer cancel()

	client := redis.NewClient(&redis.Options{Addr: testRedisAddr, DB: testRedisDB})
	defer client.Close()

	err := client.FlushDB(ctx).Err()
	require.NoError(t, err)

	action(client)

	err = client.FlushDB(ctx).Err()
	require.NoError(t, err)
}

func seedRedisStream(t *testing.T, client redis.UniversalClient, ctx context.Context, queue, jobSetId string, entryCount int) string {
	t.Helper()

	streamKey := fmt.Sprintf("%s:%s:%s", constants.EventStreamPrefix, queue, jobSetId)

	for i := range entryCount {
		_, err := client.XAdd(ctx, &redis.XAddArgs{
			Stream: streamKey,
			Values: map[string]interface{}{"index": i},
		}).Result()
		require.NoError(t, err)
	}
	return streamKey
}
