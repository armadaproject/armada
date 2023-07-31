ARG BASE_IMAGE=alpine:3.0
FROM ${BASE_IMAGE}
RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada
LABEL org.opencontainers.image.title=lookoutv2
LABEL org.opencontainers.image.description="Lookout V2"
LABEL org.opencontainers.image.url=https://hub.docker.com/r/gresearchdev/lookoutv2
USER armada
COPY lookoutv2 /app/
COPY config/lookoutv2/config.yaml /app/config/lookoutv2/config.yaml
COPY lookoutingesterv2 /app/
COPY config/lookoutingesterv2/ /app/config/lookoutingesterv2
WORKDIR /app
ENTRYPOINT ["./lookoutv2"]
