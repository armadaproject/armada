FROM alpine:3.10

RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada

USER armada

COPY eventingester /app/

COPY config/eventingester/config.yaml /app/config/eventingester/config.yaml

WORKDIR /app

ENTRYPOINT ["./eventingester"]
