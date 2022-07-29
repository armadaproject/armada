#!/bin/bash
# This script is intended to be run under the docker container at $ARMADADIR/build/proto/

if [ -d /dotnet ]; then
  export PATH=$PATH:/dotnet
fi
export DOTNET_CLI_TELEMETRY_OPTOUT=1

export TYPES=Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,\
Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,\
Mgoogle/protobuf/struct.proto=github.com/gogo/protobuf/types,\
Mgoogle/protobuf/empty.proto=github.com/gogo/protobuf/types,\
Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,\
Mgoogle/protobuf/wrappers.proto=github.com/gogo/protobuf/types

GEN_OUT=client/DotNet.gRPC/Armada.Client.Grpc/generated
if [ ! -d $GEN_OUT ]; then mkdir -p $GEN_OUT; fi

# protoc dotnet client
echo "--------- Processing protoc dotnet client"
protoc \
--proto_path=. \
--proto_path=/proto \
--plugin=protoc-gen-grpc=/usr/local/bin/grpc_csharp_plugin \
--grpc_out=$GEN_OUT \
--csharp_out=$GEN_OUT \
    google/api/annotations.proto \
    google/api/http.proto \
    pkg/api/event.proto pkg/api/queue.proto pkg/api/submit.proto \
    github.com/gogo/protobuf/gogoproto/gogo.proto

# proto dotnet k8s.  We need to do this in a separate step bso we can add a --csharp_opt=base_namespace=K8S.Io flag
# which forces files to be created in different directories and thus gets round multiple "generated.cs" files being created
echo "--------- Processing protoc dotnet k8s"
protoc \
--proto_path=/proto \
--csharp_out=$GEN_OUT \
--csharp_opt=base_namespace=K8S.Io \
    k8s.io/api/core/v1/generated.proto \
    k8s.io/apimachinery/pkg/api/resource/generated.proto \
    k8s.io/apimachinery/pkg/apis/meta/v1/generated.proto \
    k8s.io/apimachinery/pkg/runtime/generated.proto \
    k8s.io/apimachinery/pkg/runtime/schema/generated.proto \
    k8s.io/apimachinery/pkg/util/intstr/generated.proto \
    k8s.io/api/networking/v1/generated.proto



# Create NuGet package
if [ -d ./dotnet ] ; then PATH=$PATH:./dotnet; fi

export DOTNET_NOLOGO=true

cd client/DotNet.gRPC && \
  dotnet pack --include-source --include-symbols Armada.Client.Grpc
