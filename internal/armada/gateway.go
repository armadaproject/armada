package armada

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/go-openapi/runtime/middleware"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"github.com/G-Research/armada/internal/armada/api"
	protoutil "github.com/G-Research/armada/internal/armada/protoutils"
)

func ServeGateway(port uint16, grpcPort uint16) (shutdown func()) {

	address := fmt.Sprintf(":%d", port)
	grpcAddress := fmt.Sprintf(":%d", grpcPort)

	connectionCtx, cancelConnectionCtx := context.WithCancel(context.Background())

	mux := http.NewServeMux()

	m := new(protoutil.JSONMarshaller)
	gw := gwruntime.NewServeMux(gwruntime.WithMarshalerOption(gwruntime.MIMEWildcard, m))

	conn, err := grpc.DialContext(connectionCtx, grpcAddress, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	err = api.RegisterSubmitHandler(connectionCtx, gw, conn)
	if err != nil {
		panic(err)
	}
	err = api.RegisterEventHandler(connectionCtx, gw, conn)
	if err != nil {
		panic(err)
	}
	mux.Handle("/", gw)

	h := middleware.Redoc(middleware.RedocOpts{}, mux)
	h = middleware.Spec("/", []byte(api.SwaggerJsonTemplate()), h)

	srv := &http.Server{Addr: address, Handler: h}

	go func() {
		log.Printf("Gateway listening on %d", port)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			panic(err)
		}
	}()
	return func() {
		cancelConnectionCtx()
		conn.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		log.Print("Stopping gateway server")
		e := srv.Shutdown(ctx)
		if e != nil {
			panic(e)
		}
	}
}
