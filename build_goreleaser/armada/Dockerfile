ARG BASE_IMAGE=alpine:3.17.0
ARG NODE_BUILD_IMAGE=node:16.14-buster
ARG OPENAPI_BUILD_IMAGE=openapitools/openapi-generator-cli:v5.4.0

# Lookout UI
FROM ${OPENAPI_BUILD_IMAGE} AS OPENAPI
COPY internal/lookout/ui /project/internal/lookout/ui
COPY pkg/api/lookout/*.swagger.json /project/pkg/api/lookout/
COPY pkg/api/*.swagger.json /project/pkg/api/
COPY pkg/api/binoculars/*.swagger.json /project/pkg/api/binoculars/
RUN ./project/internal/lookout/ui/openapi.sh

# Lookout UI
FROM ${NODE_BUILD_IMAGE} AS NODE
COPY --from=OPENAPI /project/internal/lookout/ui /ui/
WORKDIR /ui
RUN yarn install --immutable
RUN yarn run build


FROM ${BASE_IMAGE}
RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada
USER armada

# Server
COPY server /app/
COPY config/armada/config.yaml /app/config/armada/config.yaml

# Executor
COPY executor /app/
COPY config/executor/config.yaml /app/config/executor/config.yaml

# Binoculars
COPY binoculars /app/
COPY config/binoculars/config.yaml /app/config/binoculars/config.yaml

# Lookout
COPY --from=NODE /ui/build/ /app/internal/lookout/ui/build
COPY lookout /app/
COPY config/lookout/ /app/config/lookout

# Lookout ingester
COPY lookoutingester /app/
COPY config/lookoutingester/config.yaml /app/config/lookoutingester/config.yaml

# Event ingester
COPY eventingester /app/
COPY config/eventingester/config.yaml /app/config/eventingester/config.yaml

# LookoutV2 ingester
COPY lookoutingesterv2 /app/
COPY config/lookoutingesterv2/config.yaml /app/config/lookoutingesterv2/config.yaml

# Lookoutv2
COPY lookoutv2 /app/
COPY config/lookoutv2/config.yaml /app/config/lookoutv2/config.yaml

# Lookout w/ UI
COPY lookout /app/
COPY config/lookout/config.yaml /app/config/lookout/config.yaml

# Jobservice
COPY jobservice /app/
COPY config/jobservice/config.yaml /app/config/jobservice/config.yaml

WORKDIR /app
