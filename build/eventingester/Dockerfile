FROM alpine:3.18.2

RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada

USER armada

COPY ./eventingester /app/

COPY ./config/ /app/config/eventingester

WORKDIR /app

ENTRYPOINT ["./eventingester"]
