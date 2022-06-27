#!/bin/bash
# This script is intended to be run under the docker container at $ARMADADIR/build/python-api-client/

# make the python package armada.client, not pkg.api
mkdir -p proto/armada
cp pkg/api/event.proto pkg/api/queue.proto pkg/api/submit.proto pkg/api/usage.proto proto/armada
sed -i 's/\([^\/]\)pkg\/api/\1armada/g' proto/armada/*.proto


# generate python stubs
cd proto 
python3 -m grpc_tools.protoc -I. --python_out=../client/python/armada_client --grpc_python_out=../client/python/armada_client \
    google/api/annotations.proto \
    google/api/http.proto \
    armada/event.proto armada/queue.proto armada/submit.proto armada/usage.proto \
    github.com/gogo/protobuf/gogoproto/gogo.proto \
    k8s.io/api/core/v1/generated.proto \
    k8s.io/apimachinery/pkg/api/resource/generated.proto \
    k8s.io/apimachinery/pkg/apis/meta/v1/generated.proto \
    k8s.io/apimachinery/pkg/runtime/generated.proto \
    k8s.io/apimachinery/pkg/runtime/schema/generated.proto \
    k8s.io/apimachinery/pkg/util/intstr/generated.proto \
    k8s.io/api/networking/v1/generated.proto

cd ..
# This hideous code is because we can't use python package option in grpc.
# See https://github.com/protocolbuffers/protobuf/issues/7061 for an explanation.
# We need to import these packages as a module.
sed -i 's/from armada/from armada_client.armada/g' client/python/armada_client/armada/*.py
sed -i 's/from github.com/from armada_client.github.com/g' client/python/armada_client/armada/*.py
sed -i 's/from google.api/from armada_client.google.api/g' client/python/armada_client/armada/*.py
sed -i 's/from google.api/from armada_client.google.api/g' client/python/armada_client/google/api/*.py

find client/python/armada_client/ -name '*.py' | xargs sed -i 's/from k8s.io/from armada_client.k8s.io/g'

cd client/python/armada_client
poetry install
poetry build
