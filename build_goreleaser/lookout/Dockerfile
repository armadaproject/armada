ARG NODE_BUILD_IMAGE=node:16.14-buster
ARG OPENAI_BUILD_IMAGE=openapitools/openapi-generator-cli:v5.4.0
ARG BASE_IMAGE=alpine:3.17.0

FROM ${NODE_BUILD_IMAGE} AS NODE
COPY ./internal/lookout /build/
WORKDIR /build
RUN yarn install --immutable
RUN ui/openapi.sh
RUN yarn run build

# FROM ${OPENAI_BUILD_IMAGE} AS OPENAI
# COPY --from=NODE /build /build
# WORKDIR /build
# RUN ui/openapi.sh

# FROM ${NODE_BUILD_IMAGE} AS NODE2
# COPY --from=NODE /build /build
# WORKDIR /build
# RUN yarn run build

FROM ${BASE_IMAGE}
RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada
USER armada
COPY --from=NODE /build/ui/build/ /app/internal/lookout/ui/build
COPY lookout /app/
COPY config/lookout/ /app/config/lookout
WORKDIR /app
ENTRYPOINT ["./lookout"]
