//go:build mage
// +build mage

package main

import (
	"archive/zip"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"

	goreleaserConfig "github.com/goreleaser/goreleaser/pkg/config"
	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	"sigs.k8s.io/yaml"

	"github.com/G-Research/armada/pkg/client/util"
)

const PROTOC_VERSION_MIN = "3.21.8"
const PROTOC_VERSION_DOWNLOAD = "21.8" // The "3." is omitted.

// Build images, spin up a test environment, and run the integration tests against it.
func CiIntegrationTests() error {
	mg.Deps(BootstrapTools)
	mg.Deps(Kind, DockerBundle)
	err := sh.Run("docker-compose", "up", "-d", "server", "executor")
	if err != nil {
		return err
	}
	err = sh.Run("go", "run", "cmd/armadactl/main.go", "create", "queue", "e2e-test-queue")
	if err != nil {
		return err
	}
	err = sh.Run(
		"go", "run", "cmd/testsuite/main.go", "test",
		"--tests", "testsuite/testcases/basic/*",
		"--junit", "junit.xml",
	)
	if err != nil {
		return err
	}
	return nil
}

func DockerBundle() error {
	mg.Deps(DockerBundleGoreleaserConfig)
	return sh.Run(
		"goreleaser", "release",
		"--snapshot",
		"--rm-dist",
		"-f", ".goreleaser-docker.yml",
	)
}

// Write a minimal goreleaser config to .goreleaser-docker.yml
// containing only the subset of targets in .goreleaser.yaml necessary
// for building a set of specified Docker images.
func DockerBundleGoreleaserConfig() error {
	// Docker targets to build and the build targets necessary to do so.
	dockerIds := map[string]bool{
		"bundle": true,
	}
	buildIds := map[string]bool{
		"server":          true,
		"executor":        true,
		"binoculars":      true,
		"lookoutingester": true,
		"eventingester":   true,
	}

	goreleaserConfigPath := "./.goreleaser.yml"
	config := goreleaserConfig.Project{}
	if err := util.BindJsonOrYaml(goreleaserConfigPath, &config); err != nil {
		return err
	}

	dockers := make([]goreleaserConfig.Docker, 0)
	for _, docker := range config.Dockers {
		if dockerIds[docker.ID] {
			dockers = append(dockers, docker)
		}
	}
	if len(dockers) == 0 {
		return errors.Errorf("%v matched no dockers in %s", dockerIds, goreleaserConfigPath)
	}

	builds := make([]goreleaserConfig.Build, 0)
	for _, build := range config.Builds {
		if buildIds[build.ID] {
			builds = append(builds, build)
		}
	}
	if len(builds) == 0 {
		return errors.Errorf("%v matched no builds in %s", buildIds, goreleaserConfigPath)
	}

	targets := make(map[string]interface{})
	for _, docker := range dockers {
		targets[fmt.Sprintf("%s_%s", docker.Goos, docker.Goarch)] = true
	}
	for i := range builds {
		builds[i].Goos = nil
		builds[i].Goarch = nil
		builds[i].Targets = maps.Keys(targets)
	}

	minimalConfig := goreleaserConfig.Project{
		ProjectName: config.ProjectName,
		Dist:        config.Dist,
		GoMod:       config.GoMod,
		Env:         config.Env,
		Builds:      builds,
		Dockers:     dockers,
	}
	bytes, err := yaml.Marshal(minimalConfig)
	if err != nil {
		return err
	}
	f, err := os.Create(".goreleaser-docker.yml")
	if err != nil {
		return err
	}
	defer f.Close()
	if n, err := f.Write(bytes); err != nil {
		return err
	} else if n != len(bytes) {
		return errors.Errorf("only $d out of $d bytes were written", n, len(bytes))
	}

	return nil
}

func Kind() error {
	mg.Deps(KindSetup)
	mg.Deps(KindWriteKubeConfig)
	mg.Deps(KindWaitUntilReady)
	return nil
}

func KindSetup() error {
	out, err := sh.Output("kind", "get", "clusters")
	if err != nil {
		return err
	}
	if strings.Contains(out, "armada-test") {
		return nil
	}
	if err := sh.Run("kind", "create", "cluster", "--config", "e2e/setup/kind.yaml"); err != nil {
		return err
	}

	// Images that need to be available in the Kind cluster,
	// e.g., images required for e2e tests.
	images := []string{
		"alpine:3.10",
		"nginx:1.21.6",
		"bitnami/kubectl:1.24.8",
		"registry.k8s.io/ingress-nginx/controller:v1.4.0",
		"registry.k8s.io/ingress-nginx/kube-webhook-certgen:v20220916-gd32f8c343",
	}
	for _, image := range images {
		if err := sh.Run("docker", "pull", image); err != nil {
			return err
		}
	}
	for _, image := range images {
		if err := sh.Run("kind", "load", "docker-image", image, "--name", "armada-test"); err != nil {
			return err
		}
	}

	// Resources to create in the Kind cluster.
	resources := []string{
		"e2e/setup/ingress-nginx.yaml",
		"e2e/setup/priorityclasses.yaml",
		"e2e/setup/namespace-with-anonymous-user.yaml",
	}
	for _, f := range resources {
		if err := sh.Run("kubectl", "apply", "-f", f, "--context", "kind-armada-test"); err != nil {
			return err
		}
	}

	return nil
}

// Write kubeconfig to disk.
// Needed by the executor to interact with the cluster.
func KindWriteKubeConfig() error {
	out, err := sh.Output("kind", "get", "kubeconfig", "--name", "armada-test")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(".kube/external/", os.ModeDir); err != nil {
		return err
	}
	if f, err := os.Create(".kube/external/config"); err != nil {
		return err
	} else {
		defer f.Close()
		if _, err := f.WriteString(out); err != nil {
			return err
		}
	}

	out, err = sh.Output("kind", "get", "kubeconfig", "--internal", "--name", "armada-test")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(".kube/internal/", os.ModeDir); err != nil {
		return err
	}
	if f, err := os.Create(".kube/internal/config"); err != nil {
		return err
	} else {
		defer f.Close()
		if _, err := f.WriteString(out); err != nil {
			return err
		}
	}
	return nil
}

func KindWaitUntilReady() error {
	return sh.Run(
		"kubectl", "wait", "--namespace", "ingress-nginx",
		"--for=condition=ready", "pod",
		"--selector=app.kubernetes.io/component=controller",
		"--timeout=2m",
		"--context", "kind-armada-test",
	)
}

func KindTeardown() error {
	return sh.Run("kind", "delete", "cluster", "--name", "armada-test")
}

// Clean up after yourself
func Clean() {
	fmt.Println("Cleaning...")
	for _, path := range []string{"proto", "protoc", "protoc.zip"} {
		os.RemoveAll(path)
	}
}

func Sql() error {
	return sh.Run("sqlc", "generate", "-f", "internal/scheduler/sql/sql.yaml")
}

func BootstrapTools() error {
	packages, err := sh.Output("go", "list", "-f", "{{range .Imports}}{{.}} {{end}}", "internal/tools/tools.go")
	if err != nil {
		return err
	}
	for _, p := range strings.Split(strings.TrimSpace(packages), " ") {
		if err := sh.Run("go", "install", p); err != nil {
			return err
		}
	}
	return nil
}

func Proto() error {
	mg.Deps(ProtocBootstrap)
	mg.Deps(ProtoBootstrap)
	mg.Deps(GogoBootstrap)
	cmd, err := protocOutCmd()
	if err != nil {
		return err
	}

	paths := []string{
		"pkg/api/*.proto",
		"pkg/armadaevents/*.proto",
		"internal/scheduler/schedulerobjects/*.proto",
		"pkg/api/lookout/*.proto",
		"pkg/api/binoculars/*.proto",
		"pkg/api/jobservice/*.proto",
	}
	for _, path := range paths {
		_, err = cmd(
			"--proto_path=.",
			"--proto_path=proto",
			fmt.Sprintf("--armada_out=%s,plugins=grpc:./", protoGoPackageArgs()),
			path,
		)
		if err != nil {
			return err
		}
	}

	_, err = cmd(
		"--proto_path=.",
		"--proto_path=proto",
		fmt.Sprintf("--grpc-gateway_out=logtostderr=true,%s:.", protoGoPackageArgs()),
		fmt.Sprintf("--swagger_out=logtostderr=true,%s,allow_merge=true,simple_operation_ids=true,json_names_for_fields=true,merge_file_name=./pkg/api/api:.", protoGoPackageArgs()),
		"pkg/api/event.proto",
		"pkg/api/submit.proto",
	)
	if err != nil {
		return err
	}

	_, err = cmd(
		"--proto_path=.",
		"--proto_path=proto",
		fmt.Sprintf("--grpc-gateway_out=logtostderr=true,%s:.", protoGoPackageArgs()),
		fmt.Sprintf("--swagger_out=logtostderr=true,%s,allow_merge=true,simple_operation_ids=true,json_names_for_fields=true,merge_file_name=./pkg/api/lookout/api:.", protoGoPackageArgs()),
		"pkg/api/lookout/lookout.proto",
	)
	if err != nil {
		return err
	}

	_, err = cmd(
		"--proto_path=.",
		"--proto_path=proto",
		fmt.Sprintf("--grpc-gateway_out=logtostderr=true,%s:.", protoGoPackageArgs()),
		fmt.Sprintf("--swagger_out=logtostderr=true,%s,allow_merge=true,simple_operation_ids=true,json_names_for_fields=true,merge_file_name=./pkg/api/binoculars/api:.", protoGoPackageArgs()),
		"pkg/api/binoculars/binoculars.proto",
	)
	if err != nil {
		return err
	}

	return nil
}

func GogoBootstrap() error {
	return sh.Run("go", "install", "./protoc-gen-armada.go")
}

func ProtoBootstrap() error {
	// Go modules containing .proto dependencies we need.
	modules := []string{
		"github.com/gogo/protobuf",
		"github.com/grpc-ecosystem/grpc-gateway",
		"k8s.io/api",
		"k8s.io/apimachinery",
	}
	redirects := map[string]string{
		filepath.Join("/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis/google/api/annotations.proto"): filepath.Join("/google/api/annotations.proto"),
		filepath.Join("/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis/google/api/http.proto"):        filepath.Join("/google/api/http.proto"),
		filepath.Join("/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis/google/api/httpbody.proto"):    filepath.Join("/google/api/httpbody.proto"),
	}
	for _, module := range modules {
		v, err := moduleVersion(module)
		if err != nil {
			return err
		}
		tokens := []string{os.Getenv("GOPATH"), "pkg", "mod"}
		prefix := filepath.Join(tokens...)
		tokens = append(tokens, strings.Split(module, "/")...)
		tokens[len(tokens)-1] = tokens[len(tokens)-1] + "@" + v
		err = filepath.WalkDir(filepath.Join(tokens...), func(path string, d fs.DirEntry, err error) error {
			if d.IsDir() || filepath.Ext(path) != ".proto" {
				return nil
			}
			s := path
			s = strings.ReplaceAll(s, "@"+v, "")
			s = strings.TrimPrefix(s, prefix)
			if redirect, ok := redirects[s]; ok {
				s = redirect
			}
			dstPath := filepath.Join("./proto", s)
			return copy(path, dstPath)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func protoGoPackageArgs() string {
	var rv string
	rv += "Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,"
	rv += "Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,"
	rv += "Mgoogle/protobuf/struct.proto=github.com/gogo/protobuf/types,"
	rv += "Mgoogle/protobuf/empty.proto=github.com/gogo/protobuf/types,"
	rv += "Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,"
	rv += "Mgoogle/protobuf/wrappers.proto=github.com/gogo/protobuf/types`"
	return rv
}

func moduleVersion(module string) (string, error) {
	out, err := sh.Output("go", "list", "-m", module)
	if err != nil {
		return "", err
	}
	return strings.Split(out, " ")[1], nil
}

func ProtocBootstrap() error {
	mg.Deps(ProtocInstall)
	mg.Deps(ProtocVersionCheck)
	err := filepath.WalkDir("./protoc", func(path string, d fs.DirEntry, err error) error {
		if d.IsDir() || filepath.Ext(path) != ".proto" {
			return nil
		}
		s := path
		s = strings.TrimPrefix(s, filepath.Join("protoc", "include"))
		dstPath := filepath.Join("./proto", s)
		return copy(path, dstPath)
	})
	if err != nil {
		return err
	}
	return nil
}

func ProtocVersionCheck() error {
	cmd, err := protocOutCmd()
	if err != nil {
		return err
	}
	out, err := cmd("--version")
	if err != nil {
		return err
	}
	protocVersion := strings.TrimSpace(strings.Split(out, " ")[1])
	if len(strings.Split(protocVersion, ".")) != 3 {
		return errors.Errorf("expected a semver but got %s", protocVersion)
	}
	if protocVersion < PROTOC_VERSION_MIN {
		return errors.Errorf("found protoc version %s but the minimum required is %s", protocVersion, PROTOC_VERSION_MIN)
	}
	return nil
}

func ProtocInstall() error {
	if ok, err := exists("./protoc"); ok || err != nil {
		return err
	}

	archOs, err := protocArchOs()
	if err != nil {
		return err
	}
	url := fmt.Sprintf(
		"https://github.com/protocolbuffers/protobuf/releases/download/v%s/protoc-%s-%s.zip",
		PROTOC_VERSION_DOWNLOAD, PROTOC_VERSION_DOWNLOAD, archOs,
	)

	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.ContentLength < 1000 {
		contents, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return errors.Errorf("failed to download protoc: unexpected response '%s'", string(contents))
	}

	f, err := os.Create("protoc.zip")
	if err != nil {
		return err
	}
	_, err = io.Copy(f, resp.Body)
	if err != nil {
		return err
	}
	defer os.RemoveAll("protoc.zip")

	return unzip("protoc.zip", "./protoc")
}

func unzip(zipPath, dstPath string) error {
	read, err := zip.OpenReader(zipPath)
	if err != nil {
		return err
	}
	defer read.Close()
	for _, file := range read.File {
		name := path.Join(dstPath, file.Name)
		if file.Mode().IsDir() {
			os.MkdirAll(path.Dir(name), os.ModeDir)
			continue
		}
		open, err := file.Open()
		if err != nil {
			return err
		}
		defer open.Close()
		os.MkdirAll(path.Dir(name), os.ModeDir)
		create, err := os.Create(name)
		if err != nil {
			return err
		}
		defer create.Close()
		_, err = io.Copy(create, open)
		if err != nil {
			return err
		}
	}
	return nil
}

func copy(srcPath, dstPath string) error {
	os.MkdirAll(filepath.Dir(dstPath), os.ModeDir)
	src, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	dst, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	_, err = io.Copy(dst, src)
	return err
}

func protocOutCmd() (func(args ...string) (string, error), error) {
	var protocPath string
	if runtime.GOOS == "windows" {
		protocPath = "./protoc/bin/protoc.exe"
	} else {
		protocPath = "./protoc/bin/protoc"
	}
	ok, err := exists(protocPath)
	if err != nil {
		return nil, err
	}
	if ok {
		return sh.OutCmd(protocPath), nil
	}
	return sh.OutCmd("protoc"), nil
}

func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	return false, err
}

func protocArchOs() (string, error) {
	switch runtime.GOOS + "/" + runtime.GOARCH {
	case "darwin/amd64":
		return "osx-x86_64", nil
	case "darwin/arm64":
		return "osx-aarch_64", nil
	case "linux/386":
		return "linux-x86_32", nil
	case "linux/amd64":
		return "linux-x86_64", nil
	case "linux/arm64":
		return "linux-aarch_64", nil
	case "windows/386":
		return "win32", nil
	case "windows/amd64":
		return "win64", nil
	}
	return "", errors.Errorf("protoc not supported on %s/%s", runtime.GOOS, runtime.GOARCH)
}
