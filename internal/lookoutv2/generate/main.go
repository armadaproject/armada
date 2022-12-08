package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	log "github.com/sirupsen/logrus"
)

const (
	armadaMountDir  = "/mnt/armada"
	lookoutPath     = "internal/lookoutv2"
	swaggerGenDir   = "gen"
	swaggerFilePath = "swagger.yaml"

	statikDir = "./schema/"
)

func generateSwagger() error {
	err := os.RemoveAll("./gen")
	if err != nil {
		return err
	}
	err = os.Mkdir("./gen", 0o755)
	if err != nil {
		return err
	}

	executable := "swagger"
	args := []string{"generate", "server", "-t", "./gen/", "-f", "./swagger.yaml", "--exclude-main", "-A", "lookout"}
	return run(executable, args...)
}

func generateSwaggerDocker() error {
	localSwaggerGenDir := filepath.Join(".", swaggerGenDir)
	err := os.RemoveAll(localSwaggerGenDir)
	if err != nil {
		return err
	}
	err = os.Mkdir(localSwaggerGenDir, 0o755)
	if err != nil {
		return err
	}

	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	armadaDir, err := filepath.Abs(fmt.Sprintf("%s/../..", cwd))
	if err != nil {
		return err
	}
	uid := os.Getuid()
	gid := os.Getgid()

	ctx := context.Background()
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return err
	}
	defer func() {
		err = cli.Close()
	}()

	image := "quay.io/goswagger/swagger:v0.29.0"
	containerSwaggerGenDir := filepath.Join(armadaMountDir, lookoutPath, swaggerGenDir)
	containerSwaggerFilePath := filepath.Join(armadaMountDir, lookoutPath, swaggerFilePath)
	args := []string{"generate", "server", "-t", containerSwaggerGenDir, "-f", containerSwaggerFilePath, "--exclude-main", "-A", "lookout"}

	reader, err := cli.ImagePull(ctx, image, types.ImagePullOptions{})
	if err != nil {
		return err
	}
	_, err = io.Copy(os.Stdout, reader)
	if err != nil {
		return err
	}

	res, err := cli.ContainerCreate(ctx, &container.Config{
		Image:      image,
		Cmd:        args,
		WorkingDir: armadaMountDir,
		User:       fmt.Sprintf("%d:%d", uid, gid),
	},
		&container.HostConfig{
			Mounts: []mount.Mount{
				{
					Type:   mount.TypeBind,
					Source: armadaDir,
					Target: armadaMountDir,
				},
			},
		}, nil, nil, "")
	if err != nil {
		return err
	}
	if err := cli.ContainerStart(ctx, res.ID, types.ContainerStartOptions{}); err != nil {
		return err
	}
	statusCh, errCh := cli.ContainerWait(ctx, res.ID, container.WaitConditionNotRunning)
	select {
	case err := <-errCh:
		if err != nil {
			return err
		}
	case status := <-statusCh:
		fmt.Println(status)
	}
	out, err := cli.ContainerLogs(ctx, res.ID, types.ContainerLogsOptions{ShowStdout: true, ShowStderr: true})
	if err != nil {
		return err
	}
	_, err = stdcopy.StdCopy(os.Stdout, os.Stderr, out)
	if err != nil {
		return err
	}
	err = cli.ContainerRemove(ctx, res.ID, types.ContainerRemoveOptions{
		RemoveVolumes: true,
	})
	return err
}

func generateStatik() error {
	executable := "go"
	args := []string{
		"run",
		"github.com/rakyll/statik",
		"-dest",
		statikDir,
		"-src",
		statikDir,
		"-include=*.sql",
		"-ns=lookoutv2/sql",
		"-Z",
		"-f",
		"-m",
	}
	return run(executable, args...)
}

func run(executable string, args ...string) error {
	var outBuffer, errBuffer bytes.Buffer
	cmd := exec.Command(executable, args...)
	cmd.Stdout = &outBuffer
	cmd.Stderr = &errBuffer
	err := cmd.Run()
	if len(outBuffer.String()) > 0 {
		fmt.Println(outBuffer.String())
	}
	if len(errBuffer.String()) > 0 {
		fmt.Println(errBuffer.String())
	}
	if err != nil {
		return err
	}
	return nil
}

func main() {
	err1 := generateSwagger()
	err2 := generateStatik()
	if err1 != nil {
		log.WithError(err1).Error("swagger generation failed")
	}
	if err2 != nil {
		log.WithError(err2).Error("statik generation failed")
	}
}
