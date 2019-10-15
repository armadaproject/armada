FROM alpine:3.10

COPY ./bin/linux/server /app/
COPY /config/armada/ /app/config/armada

WORKDIR /app

ENTRYPOINT ["./armada"]
