protoc -I=./ -I=$GOPATH/src -I=$GOPATH/src/github.com/gogo/protobuf/protobuf \
  -I$GOPATH/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis \
  --gogofaster_out=\
Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,\
Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,\
Mgoogle/protobuf/struct.proto=github.com/gogo/protobuf/types,\
Mgoogle/protobuf/empty.proto=github.com/gogo/protobuf/types,\
Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,\
Mgoogle/protobuf/wrappers.proto=github.com/gogo/protobuf/types,plugins=grpc:./ \
./internal/armada/api/*.proto

# gRPC Gateway + swagger
protoc -I=./ -I=$GOPATH/src -I=$GOPATH/src/github.com/gogo/protobuf/protobuf \
  -I$GOPATH/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis \
  --grpc-gateway_out=logtostderr=true:. \
  --swagger_out=logtostderr=true,allow_merge=true,merge_file_name=./internal/armada/api/api:. \
  ./internal/armada/api/event.proto \
  ./internal/armada/api/submit.proto

# Hack:
# Easiest way to make ndjson streaming work in generated clients is to pretend the stream is actually a file
# Generated resourceQuantity type needs to be fixed to be string instead of object
cat internal/armada/api/api.swagger.json | \
 jq '.["x-stream-definitions"].apiEventStreamMessage.type = "file" | .paths["/v1/job-set/{Id}"].post.produces = ["application/ndjson-stream"] | .definitions.resourceQuantity.type = "string" | del(.definitions.resourceQuantity.properties)' \
>  internal/armada/api/api.swagger.stream_as_file.json
mv internal/armada/api/api.swagger.stream_as_file.json internal/armada/api/api.swagger.json

# Embed swagger json into go binary
go run github.com/wlbr/templify -e -p=api -f=SwaggerJson  internal/armada/api/api.swagger.json

# Fix all imports ordering
go run golang.org/x/tools/cmd/goimports -w -local "github.com/G-Research/armada" ./internal/armada/api/


