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

protoc \
--proto_path=. \
--proto_path=/proto \
--gogofaster_out=$TYPES,plugins=grpc:./ \
pkg/api/jobservice/*.proto

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

# protoc dotnet client
echo "--------- Processing protoc dotnet client"
protoc \
--proto_path=. \
--proto_path=/proto \
--plugin=protoc-gen-grpc=/usr/local/bin/grpc_csharp_plugin \
--grpc_out=client/DotNet.gRPC/Armada.Client.Grpc/generated \
--csharp_out=client/DotNet.gRPC/Armada.Client.Grpc/generated \
    google/api/annotations.proto \
    google/api/http.proto \
    pkg/api/event.proto pkg/api/queue.proto pkg/api/submit.proto \
    github.com/gogo/protobuf/gogoproto/gogo.proto

# proto dotnet k8s.  We need to do this in a separate step bso we can add a --csharp_opt=base_namespace=K8S.Io flag
# which forces files to be created in different directories and thus gets round multiple "generated.cs" files being created
echo "--------- Processing protoc dotnet k8s"
protoc \
--proto_path=/proto \
--csharp_out=client/DotNet.gRPC/Armada.Client.Grpc/generated \
--csharp_opt=base_namespace=K8S.Io \
    k8s.io/api/core/v1/generated.proto \
    k8s.io/apimachinery/pkg/api/resource/generated.proto \
    k8s.io/apimachinery/pkg/apis/meta/v1/generated.proto \
    k8s.io/apimachinery/pkg/runtime/generated.proto \
    k8s.io/apimachinery/pkg/runtime/schema/generated.proto \
    k8s.io/apimachinery/pkg/util/intstr/generated.proto \
    k8s.io/api/networking/v1/generated.proto


