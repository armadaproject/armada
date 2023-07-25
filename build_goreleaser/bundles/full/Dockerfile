ARG NODE_BUILD_IMAGE=node:16.14-buster
ARG OPENAPI_BUILD_IMAGE=openapitools/openapi-generator-cli:v5.4.0
ARG BASE_IMAGE=alpine:3.18.2.0

FROM ${OPENAPI_BUILD_IMAGE} AS OPENAPI
LABEL org.opencontainers.image.title=armada-full-bundle
LABEL org.opencontainers.image.description="Armada Full Bundle"
LABEL org.opencontainers.image.url=https://hub.docker.com/r/gresearchdev/armada-full-bundle
COPY internal/lookout/ui /project/internal/lookout/ui
COPY pkg/api/lookout/*.swagger.json /project/pkg/api/lookout/
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
USER armada
COPY --from=NODE /ui/build/ /app/internal/lookout/ui/build

COPY server executor binoculars lookout lookoutingester eventingester lookoutingesterv2 lookoutv2 jobservice scheduler scheduleringester armadactl /app/
COPY config/ /app/config/

WORKDIR /app
