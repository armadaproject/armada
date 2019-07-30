FROM golang:1.12.6 as builder

RUN mkdir /app

ADD . /app

WORKDIR /app

RUN env GOOS=linux GARCH=amd64 CGO_ENABLED=0 go build -o main cmd/armada/main.go

FROM alpine:3.10

COPY --from=builder /app/main /app/

COPY --from=builder /app/config /app/config

WORKDIR /app

CMD ["./main"]
