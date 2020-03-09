package armada

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/go-openapi/runtime/middleware"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/jcmturner/gokrb5/v8/spnego"
	"google.golang.org/grpc"

	protoutil "github.com/G-Research/armada/internal/armada/protoutils"
	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/pkg/api"
)

func ServeGateway(port uint16, grpcPort uint16) (shutdown func()) {

	grpcAddress := fmt.Sprintf(":%d", grpcPort)
	connectionCtx, cancelConnectionCtx := context.WithCancel(context.Background())

	mux := http.NewServeMux()

	mux.HandleFunc("/health", health)

	m := new(protoutil.JSONMarshaller)
	gw := gwruntime.NewServeMux(
		gwruntime.WithMarshalerOption(gwruntime.MIMEWildcard, m),
		gwruntime.WithOutgoingHeaderMatcher(func(key string) (string, bool) {
			if key == strings.ToLower(spnego.HTTPHeaderAuthResponse) {
				return spnego.HTTPHeaderAuthResponse, true
			}
			return fmt.Sprintf("%s%s", gwruntime.MetadataHeaderPrefix, key), true
		}))

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
