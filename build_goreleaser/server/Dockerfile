ARG BASE_IMAGE=alpine:3.17.0
FROM ${BASE_IMAGE}

RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada
USER armada

COPY server /app/
COPY config/armada/config.yaml /app/config/armada/config.yaml

WORKDIR /app

ENTRYPOINT ["./server"]
