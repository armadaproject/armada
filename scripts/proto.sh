export TYPES=Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,\
Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,\
Mgoogle/protobuf/struct.proto=github.com/gogo/protobuf/types,\
Mgoogle/protobuf/empty.proto=github.com/gogo/protobuf/types,\
Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,\
Mgoogle/protobuf/wrappers.proto=github.com/gogo/protobuf/types

# protoc go
protoc \
--proto_path=. \
--proto_path=/proto \
--gogofaster_out=$TYPES,plugins=grpc:./ \
pkg/api/*.proto

protoc \
--proto_path=. \
--proto_path=/proto \
--gogofaster_out=$TYPES:./ \
pkg/events/*.proto

protoc \
--proto_path=. \
--proto_path=/proto \
--gogofaster_out=$TYPES,plugins=grpc:./ \
pkg/api/lookout/*.proto

protoc \
--proto_path=. \
--proto_path=/proto \
--gogofaster_out=$TYPES,plugins=grpc:./ \
pkg/api/binoculars/*.proto

# gogo proto generates correct json name inside protobuf tag but wrong json tag, for example:
#   `protobuf:"bytes,1,opt,name=job_id,json=jobId,proto3" json:"job_id,omitempty"`
# this hack fixes tag as:
#   `protobuf:"bytes,1,opt,name=job_id,json=jobId,proto3" json:"jobId,omitempty"`
# TODO: Use vanity package and annotate proto files programmatically instead of this ugly regex
sed -i 's/\(json=\([^,]*\),[^"]*" json:"\)[^,]*,/\1\2,/g'  pkg/api/*.pb.go
sed -i 's/\(json=\([^,]*\),[^"]*" json:"\)[^,]*,/\1\2,/g'  pkg/api/lookout/*.pb.go
sed -i 's/\(json=\([^,]*\),[^"]*" json:"\)[^,]*,/\1\2,/g'  pkg/api/binoculars/*.pb.go

# gogo in current version does not respect go_package option and emits wrong import
sed -i 's|api "pkg/api"|api "github.com/G-Research/armada/pkg/api"|g'  pkg/api/lookout/*.pb.go
sed -i 's|api "pkg/api"|api "github.com/G-Research/armada/pkg/api"|g'  pkg/api/binoculars/*.pb.go

# protoc grpc-gateway + swagger
protoc \
--proto_path=. \
--proto_path=/proto \
--grpc-gateway_out=logtostderr=true,$TYPES:. \
--swagger_out=logtostderr=true,$TYPES,allow_merge=true,simple_operation_ids=true,json_names_for_fields=true,merge_file_name=./pkg/api/api:. \
pkg/api/event.proto \
pkg/api/submit.proto

protoc \
--proto_path=. \
--proto_path=/proto \
--grpc-gateway_out=logtostderr=true,$TYPES:. \
--swagger_out=logtostderr=true,$TYPES,allow_merge=true,simple_operation_ids=true,json_names_for_fields=true,merge_file_name=./pkg/api/lookout/api:. \
pkg/api/lookout/lookout.proto \

protoc \
--proto_path=. \
--proto_path=/proto \
--grpc-gateway_out=logtostderr=true,$TYPES:. \
--swagger_out=logtostderr=true,$TYPES,allow_merge=true,simple_operation_ids=true,json_names_for_fields=true,merge_file_name=./pkg/api/binoculars/api:. \
pkg/api/binoculars/binoculars.proto \

# generate proper swagger types (we are using standard json serializer, GRPC gateway generates protobuf json, which is not compatible)
swagger generate spec -m -o pkg/api/api.swagger.definitions.json

# combine swagger definitions
go run ./scripts/merge_swagger.go api.swagger.json > pkg/api/api.swagger.merged.json
mv pkg/api/api.swagger.merged.json pkg/api/api.swagger.json

go run ./scripts/merge_swagger.go lookout/api.swagger.json > pkg/api/lookout/api.swagger.merged.json
mv pkg/api/lookout/api.swagger.merged.json pkg/api/lookout/api.swagger.json

go run ./scripts/merge_swagger.go binoculars/api.swagger.json > pkg/api/binoculars/api.swagger.merged.json
mv pkg/api/binoculars/api.swagger.merged.json pkg/api/binoculars/api.swagger.json

rm pkg/api/api.swagger.definitions.json

# embed swagger json into go binary
templify -e -p=api -f=SwaggerJson  pkg/api/api.swagger.json
templify -e -p=lookout -f=SwaggerJson  pkg/api/lookout/api.swagger.json
templify -e -p=binoculars -f=SwaggerJson  pkg/api/binoculars/api.swagger.json

# fix all imports ordering
goimports -w -local "github.com/G-Research/armada" ./pkg/api/

# generate dotnet client to match the swagger
# dotnet build ./client/DotNet/Armada.Client /t:NSwag
