# get required packages
docker run \
-it \
--rm \
-e GOPATH=/go \
-e GO111MODULE=on \
-v $GOPATH:/go \
golang:1.14 \
go get \
github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway@v1.12.0 \
github.com/grpc-ecosystem/grpc-gateway/protoc-gen-swagger@v1.12.0 \
github.com/golang/protobuf/protoc-gen-go@v1.3.5 \
github.com/gogo/protobuf/protoc-gen-gogofaster@v1.3.1 \
github.com/gogo/protobuf/proto@v1.3.1 \
github.com/gogo/protobuf/gogoproto@v1.3.1 \
google.golang.org/grpc@v1.24.0 \
k8s.io/api@v0.17.3 \
k8s.io/apimachinery@v0.17.3

# protoc
docker run \
-it \
--rm \
-v $PWD:/app \
-v $GOPATH/bin/protoc-gen-go:/usr/local/bin/protoc-gen-go \
-v $GOPATH/bin/protoc-gen-gogofaster:/usr/local/bin/protoc-gen-gogofaster \
-v $GOPATH/bin/protoc-gen-grpc-gateway:/usr/local/bin/protoc-gen-grpc-gateway \
-v $GOPATH/bin/protoc-gen-swagger:/usr/local/bin/protoc-gen-swagger \
-v $GOPATH/pkg/mod/k8s.io/api@v0.17.3:/proto/k8s.io/api \
-v $GOPATH/pkg/mod/k8s.io/apimachinery@v0.17.3:/proto/k8s.io/apimachinery \
-v $GOPATH/pkg/mod/github.com/gogo/protobuf\@v1.3.1:/proto/github.com/gogo/protobuf \
-v $GOPATH/pkg/mod/github.com/gogo/protobuf\@v1.3.1/protobuf/google/protobuf:/proto/google/protobuf \
-v $GOPATH/pkg/mod/github.com/grpc-ecosystem/grpc-gateway\@v1.12.0/third_party/googleapis/google/api:/proto/google/api \
-v $GOPATH/pkg/mod/github.com/grpc-ecosystem/grpc-gateway\@v1.12.0/third_party/googleapis/google/rpc:/proto/google/rpc \
-w /app \
memominsk/protobuf-alpine:3.8.0 \
--proto_path=/app \
--proto_path=/proto \
--gogofaster_out=\
Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,\
Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,\
Mgoogle/protobuf/struct.proto=github.com/gogo/protobuf/types,\
Mgoogle/protobuf/empty.proto=github.com/gogo/protobuf/types,\
Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,\
Mgoogle/protobuf/wrappers.proto=github.com/gogo/protobuf/types,plugins=grpc:./ \
pkg/api/*.proto

# grpc-gateway + swagger
docker run \
-it \
--rm \
-v $PWD:/app \
-v $GOPATH/bin/protoc-gen-go:/usr/local/bin/protoc-gen-go \
-v $GOPATH/bin/protoc-gen-gogofaster:/usr/local/bin/protoc-gen-gogofaster \
-v $GOPATH/bin/protoc-gen-grpc-gateway:/usr/local/bin/protoc-gen-grpc-gateway \
-v $GOPATH/bin/protoc-gen-swagger:/usr/local/bin/protoc-gen-swagger \
-v $GOPATH/pkg/mod/k8s.io/api@v0.17.3:/proto/k8s.io/api \
-v $GOPATH/pkg/mod/k8s.io/apimachinery@v0.17.3:/proto/k8s.io/apimachinery \
-v $GOPATH/pkg/mod/github.com/gogo/protobuf\@v1.3.1:/proto/github.com/gogo/protobuf \
-v $GOPATH/pkg/mod/github.com/gogo/protobuf\@v1.3.1/protobuf/google/protobuf:/proto/google/protobuf \
-v $GOPATH/pkg/mod/github.com/grpc-ecosystem/grpc-gateway\@v1.12.0/third_party/googleapis/google/api:/proto/google/api \
-v $GOPATH/pkg/mod/github.com/grpc-ecosystem/grpc-gateway\@v1.12.0/third_party/googleapis/google/rpc:/proto/google/rpc \
-w /app \
memominsk/protobuf-alpine:3.8.0 \
--proto_path=/app \
--proto_path=/proto \
--grpc-gateway_out=logtostderr=true:. \
--swagger_out=logtostderr=true,allow_merge=true,merge_file_name=./pkg/api/api:. \
pkg/api/event.proto \
pkg/api/submit.proto

# generate proper swagger types (we are using standard json serializer, GRPC gateway generates protobuf json, which is not compatible)
docker run \
-it \
--rm \
-e GOPATH=/go \
-e GO111MODULE=on \
-v $PWD:/app \
-v $GOPATH:/go \
-w /app \
quay.io/goswagger/swagger:v0.23.0 \
generate spec -m -o ./pkg/api/api.swagger.definitions.json

# combine swagger definitions
docker run \
-it \
--rm \
-e GOPATH=/go \
-e GO111MODULE=on \
-v $PWD:/app \
-v $GOPATH:/go \
-w /app \
golang:1.14 \
go run ./scripts/merge_swagger.go > pkg/api/api.swagger.merged.json

mv pkg/api/api.swagger.merged.json pkg/api/api.swagger.json
rm pkg/api/api.swagger.definitions.json

# embed swagger json into go binary
docker run \
-it \
--rm \
-e GOPATH=/go \
-e GO111MODULE=on \
-v $PWD:/app \
-v $GOPATH:/go \
-w /app \
golang:1.14 \
go run github.com/wlbr/templify -e -p=api -f=SwaggerJson pkg/api/api.swagger.json

# fix all imports ordering
docker run \
-it \
--rm \
-e GOPATH=/go \
-e GO111MODULE=on \
-v $PWD:/app \
-v $GOPATH:/go \
-w /app \
golang:1.14 \
go run golang.org/x/tools/cmd/goimports -w -local "github.com/G-Research/armada" ./pkg/api/

# genereate dotnet client to match the swagger
mkdir -p $HOME/.nuget

docker run \
-it \
--rm \
-v $PWD:/app \
-v $HOME/.nuget:/root/.nuget \
-w /app \
mcr.microsoft.com/dotnet/core/sdk:3.1 \
dotnet build ./client/DotNet/Armada.Client /t:NSwag
