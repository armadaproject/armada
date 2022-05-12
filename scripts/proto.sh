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
pkg/armadaevents/*.proto

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

# make the python package armada.client, not pkg.api
mkdir -p armada/client
cp pkg/api/event.proto pkg/api/queue.proto pkg/api/submit.proto pkg/api/usage.proto armada/client/
sed -i 's/\([^\/]\)pkg\/api/\1armada\/client/g' armada/client/*.proto

# generate python stubs
python3 -m grpc_tools.protoc -I. -I/proto --python_out=client/python --grpc_python_out=client/python \
    /proto/google/api/annotations.proto \
    /proto/google/api/http.proto \
    armada/client/event.proto armada/client/queue.proto armada/client/submit.proto armada/client/usage.proto \
    /proto/github.com/gogo/protobuf/gogoproto/gogo.proto \
    /proto/k8s.io/api/core/v1/generated.proto \
    /proto/k8s.io/apimachinery/pkg/api/resource/generated.proto \
    /proto/k8s.io/apimachinery/pkg/apis/meta/v1/generated.proto \
    /proto/k8s.io/apimachinery/pkg/runtime/generated.proto \
    /proto/k8s.io/apimachinery/pkg/runtime/schema/generated.proto \
    /proto/k8s.io/apimachinery/pkg/util/intstr/generated.proto
