ARG NODE_BUILD_IMAGE=node:16.14-buster
ARG OPENAPI_BUILD_IMAGE=openapitools/openapi-generator-cli:v5.4.0
ARG BASE_IMAGE=alpine:3.17.0

FROM ${NODE_BUILD_IMAGE} AS YARNINSTALL
COPY internal/lookout/ui /ui/
WORKDIR /ui
RUN yarn install --immutable

FROM ${OPENAPI_BUILD_IMAGE} AS OPENAPI
COPY --from=YARNINSTALL /ui /project/internal/lookout/ui
COPY pkg/api/lookout/*.swagger.json /project/pkg/api/lookout/
COPY pkg/api/*.swagger.json /project/pkg/api/
COPY pkg/api/binoculars/*.swagger.json /project/pkg/api/binoculars/
RUN ./project/internal/lookout/ui/openapi.sh

FROM ${NODE_BUILD_IMAGE} AS YARNBUILD
COPY --from=OPENAPI /project/internal/lookout/ui /ui/
WORKDIR /ui
RUN yarn run build

FROM ${BASE_IMAGE}
RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada
USER armada
COPY --from=YARNBUILD /ui/build/ /app/internal/lookout/ui/build
COPY lookout /app/
COPY config/lookout/ /app/config/lookout
WORKDIR /app
ENTRYPOINT ["./lookout"]
