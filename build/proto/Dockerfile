# Dockerfile for building .proto files.

ARG GOLANG_VERSION=1.16
ARG GOGO_PROTOBUF_VERSION=1.3.2
ARG GRPC_GATEWAY_VERSION=1.15.0
ARG K8S_VERSION=0.17.3
ARG GO_SWAGGER_VERSION=0.23.0
ARG TEMPLIFY_VERSION=0.0.0-20190823200653-c12e62ca00c1
ARG GOIMPORTS_VERSION=0.0.0-20200529172331-a64b76657301

# We download the protoc binary from Maven. Set this argument to use an alternate Maven repo.
ARG MAVEN_URL=https://repo1.maven.org/maven2

# We check the sha1 of the downloaded protoc binary, which must be added here manually, e.g., from
# https://repo1.maven.org/maven2/com/google/protobuf/protoc/
ARG PROTOC_VERSION=3.17.3
ARG PROTOC_SHA1=17c70df7057b054a221080a8178c421b6bfd4af0

# Populate with environment variables from shell by running docker build with
# --build-arg GOPROXY --build-arg GOPRIVATE
ARG GOPROXY=""
ARG GOPRIVATE=""

FROM --platform=linux/amd64 golang:${GOLANG_VERSION}-buster as builder

# Load GOPROXY and GOPRIVATE as environment varialbes in the build container.
# If not defined, these are set to "", which defaults to the standard go package source.
ARG GOPROXY
ARG GOPRIVATE
ENV GOPROXY=$GOPROXY
ENV GOPRIVATE=$GOPRIVATE

# Copy in the protoc toolchain (which must be downloaded and made available manually).
ARG PROTOC_VERSION
ARG PROTOC_SHA1
ARG MAVEN_URL
RUN curl -OLk ${MAVEN_URL}/com/google/protobuf/protoc/${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.exe && \
    mv protoc-${PROTOC_VERSION}-linux-x86_64.exe /usr/local/bin/protoc && \
    echo "${PROTOC_SHA1} /usr/local/bin/protoc" | sha1sum -c - && \
    chmod +x /usr/local/bin/protoc

ARG GOGO_PROTOBUF_VERSION
RUN GO111MODULE=on go get github.com/gogo/protobuf/protoc-gen-gogofaster@v${GOGO_PROTOBUF_VERSION} && \
    mkdir -p /proto/github.com/gogo/protobuf && \
    cp -r /go/pkg/mod/github.com/gogo/protobuf@v${GOGO_PROTOBUF_VERSION}/* /proto/github.com/gogo/protobuf && \
    mkdir -p /proto/google && \
    cp -r /go/pkg/mod/github.com/gogo/protobuf@v${GOGO_PROTOBUF_VERSION}/protobuf/google/* /proto/google

ARG GRPC_GATEWAY_VERSION
RUN GO111MODULE=on go get \
    github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway@v${GRPC_GATEWAY_VERSION} \
    github.com/grpc-ecosystem/grpc-gateway/protoc-gen-swagger@v${GRPC_GATEWAY_VERSION} && \
    mkdir -p /proto/google && \
    cp -r /go/pkg/mod/github.com/grpc-ecosystem/grpc-gateway@v${GRPC_GATEWAY_VERSION}/third_party/googleapis/google/* /proto/google

ARG K8S_VERSION
RUN go mod download k8s.io/api@v${K8S_VERSION} && \
    go mod download k8s.io/apimachinery@v${K8S_VERSION} && \
    mkdir -p /proto/k8s.io/api && \
    mkdir -p /proto/k8s.io/apimachinery && \
    cp -r $(go env GOPATH)/pkg/mod/k8s.io/api@v${K8S_VERSION}/* /proto/k8s.io/api && \
    cp -r $(go env GOPATH)/pkg/mod/k8s.io/apimachinery@v${K8S_VERSION}/* /proto/k8s.io/apimachinery

ARG GO_SWAGGER_VERSION
RUN GO111MODULE=on go get github.com/go-swagger/go-swagger/cmd/swagger@v${GO_SWAGGER_VERSION}

ARG TEMPLIFY_VERSION
RUN GO111MODULE=on go get github.com/wlbr/templify@v${TEMPLIFY_VERSION}

ARG GOIMPORTS_VERSION
RUN GO111MODULE=on go get golang.org/x/tools/cmd/goimports@v${GOIMPORTS_VERSION}

ARG GOLANG_VERSION
FROM --platform=linux/amd64 golang:${GOLANG_VERSION}-buster
COPY --from=builder /usr/local/bin /usr/local/bin
COPY --from=builder /go/bin /usr/local/bin
COPY --from=builder /proto /proto

# dotnet
ENV \
    # Enable detection of running in a container
    DOTNET_RUNNING_IN_CONTAINER=true \
    # Enable correct mode for dotnet watch (only mode supported in a container)
    DOTNET_USE_POLLING_FILE_WATCHER=true \
    # Skip extraction of XML docs - generally not useful within an image/container - helps performance
    NUGET_XMLDOC_MODE=skip \
    # PowerShell telemetry for docker image usage
    POWERSHELL_DISTRIBUTION_CHANNEL=PSDocker-DotnetCoreSDK-Debian-10

# Install .NET CLI dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libc6 \
        libgcc1 \
        libgssapi-krb5-2 \
        libicu63 \
        libssl1.1 \
        libstdc++6 \
        zlib1g \
    && rm -rf /var/lib/apt/lists/*

# Install .NET Core SDK
RUN dotnet_sdk_version=3.1.300 \
    && curl -SL --output dotnet.tar.gz https://dotnetcli.azureedge.net/dotnet/Sdk/$dotnet_sdk_version/dotnet-sdk-$dotnet_sdk_version-linux-x64.tar.gz \
    && dotnet_sha512='1c3844ea5f8847d92372dae67529ebb08f09999cac0aa10ace571c63a9bfb615adbf8b9d5cebb2f960b0a81f6a5fba7d80edb69b195b77c2c7cca174cbc2fd0b' \
    && echo "$dotnet_sha512 dotnet.tar.gz" | sha512sum -c - \
    && mkdir -p /usr/share/dotnet \
    && tar -ozxf dotnet.tar.gz -C /usr/share/dotnet \
    && rm dotnet.tar.gz \
    && ln -s /usr/share/dotnet/dotnet /usr/bin/dotnet \
    # Trigger first run experience by running arbitrary cmd
    && dotnet help

# Install PowerShell global tool
RUN powershell_version=7.0.1 \
    && curl -SL --output PowerShell.Linux.x64.$powershell_version.nupkg https://pwshtool.blob.core.windows.net/tool/$powershell_version/PowerShell.Linux.x64.$powershell_version.nupkg \
    && powershell_sha512='b6b67b59233b3ad68e33e49eff16caeb3b1c87641b9a6cd518a19e3ff69491a8a1b3c5026635549c7fd377a902a33ca17f41b7913f66099f316882390448c3f7' \
    && echo "$powershell_sha512  PowerShell.Linux.x64.$powershell_version.nupkg" | sha512sum -c - \
    && mkdir -p /usr/share/powershell \
    && dotnet tool install --add-source / --tool-path /usr/share/powershell --version $powershell_version PowerShell.Linux.x64 \
    && dotnet nuget locals all --clear \
    && rm PowerShell.Linux.x64.$powershell_version.nupkg \
    && ln -s /usr/share/powershell/pwsh /usr/bin/pwsh \
    && chmod 755 /usr/share/powershell/pwsh \
    # To reduce image size, remove the copy nupkg that nuget keeps.
    && find /usr/share/powershell -print | grep -i '.*[.]nupkg$' | xargs rm

ENTRYPOINT ["/bin/bash"]
