# Commands to run
# docker build -t armada-demo -f .devcontainer/demo/Dockerfile .
# docker push user/armada-demo:latest

FROM golang:1.18-buster

# Server
COPY bin/server /bin/

# Executor
COPY bin/executor /bin/

# Binoculars
COPY bin/binoculars /bin/

# Lookout ingester
COPY bin/lookoutingester /bin/

# Event ingester
COPY bin/eventingester /bin/

# Lookout
COPY bin/lookout /bin/

# Lookout V2
COPY bin/lookoutv2 /bin/

# Jobservice
COPY bin/jobservice /bin/

# Lookout V2 Ingester
COPY bin/lookoutingesterv2 /bin/

# Lookout UI
COPY internal/lookout/ui/build /bin/lookoutui

WORKDIR /app
