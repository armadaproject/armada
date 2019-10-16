FROM alpine:3.10

COPY ./bin/linux/server /app/
COPY /config/armada/ /app/config/armada

RUN apk update && apk add --no-cache ca-certificates

WORKDIR /app

ENTRYPOINT ["./server"]
