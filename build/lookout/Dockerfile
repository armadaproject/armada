ARG NODE_BUILD_IMAGE=node:22.12-bullseye
ARG OPENAPI_BUILD_IMAGE=openapitools/openapi-generator-cli:v5.4.0
ARG BASE_IMAGE=alpine:3.21.0

FROM ${OPENAPI_BUILD_IMAGE} AS OPENAPI

COPY internal/lookoutui /project/internal/lookoutui
COPY pkg/api/*.swagger.json /project/pkg/api/
COPY pkg/api/binoculars/*.swagger.json /project/pkg/api/binoculars/
COPY pkg/api/schedulerobjects/*.swagger.json /project/pkg/api/schedulerobjects/
RUN ./project/internal/lookoutui/openapi.sh

FROM ${NODE_BUILD_IMAGE} AS NODE
COPY --from=OPENAPI /project/internal/lookoutui /lookoutui/
WORKDIR /lookoutui
RUN yarn install --immutable
RUN yarn build

FROM ${BASE_IMAGE}
RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada
LABEL org.opencontainers.image.title=lookoutv2
LABEL org.opencontainers.image.description="Lookout V2"
LABEL org.opencontainers.image.url=https://hub.docker.com/r/gresearch/lookoutv2
USER armada
COPY --from=NODE /lookoutui/build/ /app/internal/lookoutui/build
COPY lookout /app/
COPY config/lookout/config.yaml /app/config/lookout/config.yaml
COPY lookoutingesterv2 /app/
COPY config/lookoutingesterv2/ /app/config/lookoutingesterv2
COPY config/logging.yaml /app/config/logging.yaml
WORKDIR /app
ENTRYPOINT ["./lookout"]
