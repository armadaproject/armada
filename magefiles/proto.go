package main

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/magefile/mage/sh"
	"github.com/pkg/errors"
)

func protoInstallProtocArmadaPlugin() error {
	return goRun("install", "protoc-gen-armada.go")
}

func protoPrepareThirdPartyProtos() error {
	// Go modules containing .proto dependencies we need.
	modules := []string{
		"github.com/gogo/protobuf",
		"github.com/grpc-ecosystem/grpc-gateway",
		"k8s.io/api",
		"k8s.io/apimachinery",
	}
	redirects := map[string]string{
		filepath.Join("./github.com/gogo/protobuf/protobuf/google/protobuf/any.proto"):                                filepath.Join("./google/protobuf/any.proto"),
		filepath.Join("./github.com/gogo/protobuf/protobuf/google/protobuf/api.proto"):                                filepath.Join("./google/protobuf/api.proto"),
		filepath.Join("./github.com/gogo/protobuf/protobuf/google/protobuf/compiler/plugin.proto"):                    filepath.Join("./google/protobuf/compiler/plugin.proto"),
		filepath.Join("./github.com/gogo/protobuf/protobuf/google/protobuf/descriptor.proto"):                         filepath.Join("./google/protobuf/descriptor.proto"),
		filepath.Join("./github.com/gogo/protobuf/protobuf/google/protobuf/duration.proto"):                           filepath.Join("./google/protobuf/duration.proto"),
		filepath.Join("./github.com/gogo/protobuf/protobuf/google/protobuf/empty.proto"):                              filepath.Join("./google/protobuf/empty.proto"),
		filepath.Join("./github.com/gogo/protobuf/protobuf/google/protobuf/field_mask.proto"):                         filepath.Join("./google/protobuf/field_mask.proto"),
		filepath.Join("./github.com/gogo/protobuf/protobuf/google/protobuf/source_context.proto"):                     filepath.Join("./google/protobuf/source_context.proto"),
		filepath.Join("./github.com/gogo/protobuf/protobuf/google/protobuf/struct.proto"):                             filepath.Join("./google/protobuf/struct.proto"),
		filepath.Join("./github.com/gogo/protobuf/protobuf/google/protobuf/timestamp.proto"):                          filepath.Join("./google/protobuf/timestamp.proto"),
		filepath.Join("./github.com/gogo/protobuf/protobuf/google/protobuf/type.proto"):                               filepath.Join("./google/protobuf/type.proto"),
		filepath.Join("./github.com/gogo/protobuf/protobuf/google/protobuf/wrappers.proto"):                           filepath.Join("./google/protobuf/wrappers.proto"),
		filepath.Join("./github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis/google/api/annotations.proto"): filepath.Join("./google/api/annotations.proto"),
		filepath.Join("./github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis/google/api/http.proto"):        filepath.Join("./google/api/http.proto"),
		filepath.Join("./github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis/google/api/httpbody.proto"):    filepath.Join("./google/api/httpbody.proto"),
	}
	goPath, err := goEnv("GOPATH")
	if err != nil {
		return errors.Errorf("error getting GOPATH: %v", err)
	}
	if goPath == "" {
		return errors.New("error GOPATH is not set")
	}
	goModPath := filepath.Join(goPath, "pkg", "mod")
	for _, module := range modules {
		version, err := goModuleVersion(module)
		if err != nil {
			return err
		}
		modulePath := filepath.Join(strings.Split(module, "/")...)
		modulePathWithVersion := fmt.Sprintf("%s@%s", modulePath, version)
		err = filepath.WalkDir(filepath.Join(goModPath, modulePathWithVersion), func(path string, d fs.DirEntry, err error) error {
			if (d != nil && d.IsDir()) || filepath.Ext(path) != ".proto" {
				return nil
			}
			dest, err := filepath.Rel(goModPath, path)
			if err != nil {
				return err
			}
			dest = strings.ReplaceAll(dest, modulePathWithVersion, modulePath)
			if redirect, ok := redirects[dest]; ok {
				dest = redirect
			}
			dest = filepath.Join("./proto", dest)
			return copy(path, dest)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func copy(srcPath, dstPath string) error {
	err := os.MkdirAll(filepath.Dir(dstPath), os.ModeDir|0o755)
	if err != nil {
		return err
	}
	src, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer src.Close()
	dst, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer dst.Close()
	_, err = io.Copy(dst, src)
	return err
}

func protoGenerate() error {
	patterns := []string{
		"pkg/api/*.proto",
		"pkg/armadaevents/*.proto",
		"internal/scheduler/schedulerobjects/*.proto",
		"pkg/api/lookout/*.proto",
		"pkg/api/binoculars/*.proto",
		"pkg/api/jobservice/*.proto",
	}
	for _, pattern := range patterns {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return err
		}
		err = protoProtocRun(true, false, "", matches...)
		if err != nil {
			return err
		}
	}

	err := sh.Run("goimports", "-w", "-local", "github.com/G-Research/armada", "./pkg/api/", "./pkg/armadaevents/", "./internal/scheduler/schedulerobjects/")
	if err != nil {
		return err
	}

	err = protoProtocRun(false, true, "./pkg/api/api", "pkg/api/event.proto", "pkg/api/submit.proto")
	if err != nil {
		return err
	}

	err = protoProtocRun(false, true, "./pkg/api/lookout/api", "pkg/api/lookout/lookout.proto")
	if err != nil {
		return err
	}

	err = protoProtocRun(false, true, "./pkg/api/binoculars/api", "pkg/api/binoculars/binoculars.proto")
	if err != nil {
		return err
	}

	return nil
}

func protoProtocRun(armada, grpcGateway bool, swaggerFileName string, paths ...string) error {
	modules := "Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types," +
		"Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types," +
		"Mgoogle/protobuf/struct.proto=github.com/gogo/protobuf/types," +
		"Mgoogle/protobuf/empty.proto=github.com/gogo/protobuf/types," +
		"Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types," +
		"Mgoogle/protobuf/wrappers.proto=github.com/gogo/protobuf/types"

	args := []string{
		"--proto_path=.",
		"--proto_path=proto",
	}

	if armada {
		args = append(args, fmt.Sprintf("--armada_out=paths=source_relative,plugins=grpc,%s:./", modules))
	}

	if grpcGateway {
		args = append(args, "--grpc-gateway_out=logtostderr=true,paths=source_relative:.")
	}

	if swaggerFileName != "" {
		args = append(args, fmt.Sprintf("--swagger_out=logtostderr=true,allow_merge=true,simple_operation_ids=true,json_names_for_fields=true,merge_file_name=%s:.", swaggerFileName))
	}

	args = append(args, paths...)

	return protocRun(args...)
}
