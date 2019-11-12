package armada

import (
	"context"
	"fmt"
	"net/http"

	"github.com/go-openapi/runtime/middleware"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc"

	"github.com/G-Research/armada/internal/armada/api"
	protoutil "github.com/G-Research/armada/internal/armada/protoutils"
	"github.com/G-Research/armada/internal/common"
)

func ServeGateway(port uint16, grpcPort uint16) (shutdown func()) {

	grpcAddress := fmt.Sprintf(":%d", grpcPort)
	connectionCtx, cancelConnectionCtx := context.WithCancel(context.Background())

	mux := http.NewServeMux()

	mux.HandleFunc("/health", health)

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
	h := middleware.Spec("/", []byte(api.SwaggerJsonTemplate()), mux)

	cancel := common.ServeHttp(port, h)

	return func() {
		cancelConnectionCtx()
		conn.Close()
		cancel()
	}
}

func health(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}
