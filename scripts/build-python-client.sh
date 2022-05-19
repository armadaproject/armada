# This script is intended to be run in the docker container at $ARMADADIR/build/python-api-client/

# make the python package armada_client.generated_client, not pkg.api
mkdir -p armada_client/generated_client
cp pkg/api/event.proto pkg/api/queue.proto pkg/api/submit.proto pkg/api/usage.proto armada_client/generated_client/
sed -i 's/\([^\/]\)pkg\/api/\1armada_client\/generated_client/g' armada_client/generated_client/*.proto


# generate python stubs
python3 -m grpc_tools.protoc -I. -I/proto --python_out=client/python/armada_client --grpc_python_out=client/python/armada_client \
    /proto/google/api/annotations.proto \
    /proto/google/api/http.proto \
    /proto/github.com/gogo/protobuf/gogoproto/gogo.proto \
    /proto/k8s.io/api/core/v1/generated.proto \
    /proto/k8s.io/apimachinery/pkg/api/resource/generated.proto \
    /proto/k8s.io/apimachinery/pkg/apis/meta/v1/generated.proto \
    /proto/k8s.io/apimachinery/pkg/runtime/generated.proto \
    /proto/k8s.io/apimachinery/pkg/runtime/schema/generated.proto \
    /proto/k8s.io/apimachinery/pkg/util/intstr/generated.proto \
    /proto/k8s.io/api/networking/v1/generated.proto

python3 -m grpc_tools.protoc -I. -I/proto --python_out=client/python --grpc_python_out=client/python \
    armada_client/generated_client/event.proto armada_client/generated_client/queue.proto armada_client/generated_client/submit.proto armada_client/generated_client/usage.proto 

touch client/python/armada_client/generated_client/__init__.py
