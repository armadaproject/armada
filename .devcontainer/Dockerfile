FROM mcr.microsoft.com/devcontainers/go:2-1.25

# Install xdg-utils for OIDC login flow
RUN apt-get update && apt-get install -y xdg-utils && rm -rf /var/lib/apt/lists/*

# Install protoc
RUN ARCH=$(uname -m | sed 's/aarch64/aarch_64/' | sed 's/x86_64/x86_64/') && \
  curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v25.1/protoc-25.1-linux-${ARCH}.zip && \
  unzip -o protoc-25.1-linux-*.zip -d /usr/local && \
  chmod +x /usr/local/bin/protoc && \
  rm protoc-25.1-linux-*.zip

# Install sqlc
RUN ARCH=$(uname -m | sed 's/aarch64/arm64/' | sed 's/x86_64/amd64/') && \
  curl -L https://downloads.sqlc.dev/sqlc_1.30.0_linux_${ARCH}.tar.gz -o sqlc.tar.gz && \
  tar -xzf sqlc.tar.gz -C /usr/local/bin && \
  chmod +x /usr/local/bin/sqlc && \
  rm sqlc.tar.gz

# Install armadactl
RUN ARCH=$(uname -m | sed 's/aarch64/arm64/' | sed 's/x86_64/amd64/') && \
  ARMADA_VERSION=v0.20.28 && \
  curl -L -o /tmp/armadactl.tar.gz \
  "https://github.com/armadaproject/armada/releases/download/${ARMADA_VERSION}/armadactl_${ARMADA_VERSION#v}_linux_${ARCH}.tar.gz" && \
  tar -xzf /tmp/armadactl.tar.gz -C /tmp && \
  mv /tmp/armadactl /usr/local/bin/armadactl && \
  chmod +x /usr/local/bin/armadactl && \
  rm /tmp/armadactl.tar.gz
