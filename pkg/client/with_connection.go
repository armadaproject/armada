package client

import (
	"google.golang.org/grpc"

	"github.com/G-Research/armada/pkg/api"
)

func WithConnection(apiConnectionDetails *ApiConnectionDetails, action func(*grpc.ClientConn) error) error {
	conn, err := CreateApiConnection(apiConnectionDetails)
	if err != nil {
		return err
	}
	defer conn.Close()
	return action(conn)
}

func WithSubmitClient(apiConnectionDetails *ApiConnectionDetails, action func(api.SubmitClient) error) error {
	return WithConnection(apiConnectionDetails, func(cc *grpc.ClientConn) error {
		client := api.NewSubmitClient(cc)
		return action(client)
	})
}

func WithEventClient(apiConnectionDetails *ApiConnectionDetails, action func(api.EventClient) error) error {
	return WithConnection(apiConnectionDetails, func(cc *grpc.ClientConn) error {
		client := api.NewEventClient(cc)
		return action(client)
	})
}
