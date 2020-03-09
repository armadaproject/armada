package util

import (
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"github.com/G-Research/armada/internal/common/client"
)

func WithConnection(apiConnectionDetails *client.ApiConnectionDetails, action func(*grpc.ClientConn)) {
	conn, err := client.CreateApiConnection(apiConnectionDetails)

	if err != nil {
		log.Errorf("Failed to connect to api because %s", err)
		return
	}
	defer conn.Close()

	action(conn)
}
