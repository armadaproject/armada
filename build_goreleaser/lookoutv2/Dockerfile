ARG NODE_BUILD_IMAGE=node:16.14-buster
ARG OPENAPI_BUILD_IMAGE=openapitools/openapi-generator-cli:v5.4.0
ARG BASE_IMAGE=alpine:3.18.3

FROM ${OPENAPI_BUILD_IMAGE} AS OPENAPI

COPY internal/lookout/ui /project/internal/lookout/ui
COPY pkg/api/*.swagger.json /project/pkg/api/
COPY pkg/api/binoculars/*.swagger.json /project/pkg/api/binoculars/
RUN ./project/internal/lookout/ui/openapi.sh

FROM ${NODE_BUILD_IMAGE} AS NODE
COPY --from=OPENAPI /project/internal/lookout/ui /ui/
WORKDIR /ui
RUN yarn install --immutable
RUN yarn run build

FROM ${BASE_IMAGE}
RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada
LABEL org.opencontainers.image.title=lookoutv2
LABEL org.opencontainers.image.description="Lookout V2"
LABEL org.opencontainers.image.url=https://hub.docker.com/r/gresearchdev/lookoutv2
USER armada
COPY --from=NODE /ui/build/ /app/internal/lookout/ui/build
COPY lookoutv2 /app/
COPY config/lookoutv2/config.yaml /app/config/lookoutv2/config.yaml
COPY lookoutingesterv2 /app/
COPY config/lookoutingesterv2/ /app/config/lookoutingesterv2
WORKDIR /app
ENTRYPOINT ["./lookoutv2"]
