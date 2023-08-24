package main

import (
	"fmt"
	"net"
	"sync"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/encoding/gzip"

	"github.com/armadaproject/armada/pkg/api"
)

func ServePerformanceTestArmadaServer(port int) error {
	comp := encoding.GetCompressor(gzip.Name)
	encoding.RegisterCompressor(comp)

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

func main() {
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		err := ServePerformanceTestArmadaServer(1337)
		if err != nil {
			fmt.Println(err.Error())
		}
		wg.Done()
	}()

	wg.Wait()
}
