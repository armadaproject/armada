FROM alpine:3.10

COPY ./bin/linux/executor /app/
COPY /config/executor/ /app/config/executor

RUN apk update && apk add --no-cache ca-certificates

WORKDIR /app

RUN addgroup -S armada && adduser -S armada -G armada

RUN chown -R armada:armada /app

USER armada

ENTRYPOINT ["./executor"]
