# This script is intended to be run under the docker container at $ARMADADIR/build/python-api-client/

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
    /proto/k8s.io/apimachinery/pkg/util/intstr/generated.proto \
    /proto/k8s.io/api/networking/v1/generated.proto
