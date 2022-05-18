ARG PYTHON_VERSION=3.10.4
ARG GOGO_PROTOBUF_VERSION=1.3.2
# the commit below is the one whose protos are imported by the go package protoc-gen-grpc-gateway v1.15.0
ARG GOOGLEAPIS_COMMIT=3544ab16c3342d790b00764251e348705991ea4b
ARG K8S_VERSION=0.22.4

FROM --platform=x86_64 python:${PYTHON_VERSION}-buster

RUN mkdir /proto

ARG GOGO_PROTOBUF_VERSION
RUN git clone -c advice.detachedHead=false -b v${GOGO_PROTOBUF_VERSION} --single-branch https://github.com/gogo/protobuf.git && \
    mkdir -p /proto/github.com/gogo/protobuf && \
    cp -r protobuf/* /proto/github.com/gogo/protobuf && \
    mkdir -p /proto/google && \
    cp -r protobuf/protobuf/google/* /proto/google

ARG GOOGLEAPIS_COMMIT
RUN git clone -b master --single-branch https://github.com/googleapis/googleapis.git && \
    cd googleapis && \
    git -c advice.detachedHead=false checkout ${GOOGLEAPIS_COMMIT} && \
    cp --parents google/api/annotations.proto google/api/http.proto google/api/httpbody.proto \
        google/rpc/code.proto google/rpc/error_details.proto google/rpc/status.proto /proto/ && \
    cd ..

ARG K8S_VERSION
RUN git clone -c advice.detachedHead=false -b v${K8S_VERSION} --single-branch https://github.com/kubernetes/apimachinery.git && \
    mkdir -p /proto/k8s.io/apimachinery && \
    cd apimachinery && \
    find . -regex '.*proto$' -exec cp --parents '{}' /proto/k8s.io/apimachinery \; && \
    cd ..

ARG K8S_VERSION
RUN git clone -c advice.detachedHead=false -b v${K8S_VERSION} --single-branch https://github.com/kubernetes/api.git && \
    mkdir -p /proto/k8s.io/api && \
    cd api && \
    find . -regex '.*proto$' -exec cp --parents '{}' /proto/k8s.io/api \; && \
    cd ..

RUN pip install grpcio grpcio-tools

ENTRYPOINT ["/bin/bash"]