package client

import (
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

func WithConnection(apiConnectionDetails *ApiConnectionDetails, action func(*grpc.ClientConn)) {
	conn, err := CreateApiConnection(apiConnectionDetails)

	if err != nil {
		log.Errorf("Failed to connect to api because %s", err)
		return
	}
	defer conn.Close()

	action(conn)
}
