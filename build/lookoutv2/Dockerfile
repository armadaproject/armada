FROM alpine:3.14

RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada

USER armada

COPY ./lookoutv2 /app/

COPY ./config/ /app/config/lookoutv2

WORKDIR /app

ENTRYPOINT ["./lookoutv2"]
