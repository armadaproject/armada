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

# generate proper swagger types (we are using standard json serializer, GRPC gateway generates protobuf json, which is not compatible)
go run github.com/go-swagger/go-swagger/cmd/swagger generate spec -m -o internal/armada/api/api.swagger.definitions.json

# combine swagger definitions
go run ./scripts/merge_swagger.go > internal/armada/api/api.swagger.merged.json

mv internal/armada/api/api.swagger.merged.json internal/armada/api/api.swagger.json
rm internal/armada/api/api.swagger.definitions.json

# Embed swagger json into go binary
go run github.com/wlbr/templify -e -p=api -f=SwaggerJson  internal/armada/api/api.swagger.json

# Fix all imports ordering
go run golang.org/x/tools/cmd/goimports -w -local "github.com/G-Research/armada" ./internal/armada/api/


