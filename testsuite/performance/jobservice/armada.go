package main

import (
	"fmt"
	"net"

	"github.com/armadaproject/armada/pkg/api"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

func ServePerformanceTestArmadaServer(port int) error {
	server := grpc.NewServer([]grpc.ServerOption{}...)

	performanceTestEventServer := NewPerformanceTestEventServer()

	api.RegisterEventServer(server, performanceTestEventServer)

	log.Infof("Armada performanceTestEventServer gRPC server listening on %d", port)
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	return server.Serve(lis)
}
