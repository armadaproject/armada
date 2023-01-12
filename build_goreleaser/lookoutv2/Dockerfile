ARG BASE_IMAGE=alpine:3.17.0
FROM ${BASE_IMAGE}
RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada
USER armada
COPY lookoutv2 /app/
COPY config/lookoutv2/config.yaml /app/config/lookoutv2/config.yaml
COPY lookoutingesterv2 /app/
COPY config/lookoutingesterv2/ /app/config/lookoutingesterv2
WORKDIR /app
ENTRYPOINT ["./lookoutv2"]