# protoc go
protoc \
--proto_path=. \
--proto_path=/proto \
--gogofaster_out=\
Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,\
Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,\
Mgoogle/protobuf/struct.proto=github.com/gogo/protobuf/types,\
Mgoogle/protobuf/empty.proto=github.com/gogo/protobuf/types,\
Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,\
Mgoogle/protobuf/wrappers.proto=github.com/gogo/protobuf/types,\
plugins=grpc:./ \
pkg/api/*.proto

# protoc grpc-gateway + swagger
protoc \
--proto_path=. \
--proto_path=/proto \
--grpc-gateway_out=logtostderr=true:. \
--swagger_out=logtostderr=true,allow_merge=true,merge_file_name=./pkg/api/api:. \
pkg/api/event.proto \
pkg/api/submit.proto

# generate proper swagger types (we are using standard json serializer, GRPC gateway generates protobuf json, which is not compatible)
go run github.com/go-swagger/go-swagger/cmd/swagger generate spec -m -o pkg/api/api.swagger.definitions.json

# combine swagger definitions
go run ./scripts/merge_swagger.go > pkg/api/api.swagger.merged.json
mv pkg/api/api.swagger.merged.json pkg/api/api.swagger.json
rm pkg/api/api.swagger.definitions.json

# embed swagger json into go binary
go run github.com/wlbr/templify -e -p=api -f=SwaggerJson  pkg/api/api.swagger.json

# fix all imports ordering
go run golang.org/x/tools/cmd/goimports -w -local "github.com/G-Research/armada" ./pkg/api/

# generate dotnet client to match the swagger
dotnet build ./client/DotNet/Armada.Client /t:NSwag
