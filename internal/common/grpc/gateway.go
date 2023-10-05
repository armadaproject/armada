package grpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"google.golang.org/grpc/credentials"
	"net/http"
	"path"
	"strings"

	"google.golang.org/grpc/credentials/insecure"

	"github.com/go-openapi/runtime/middleware"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/jcmturner/gokrb5/v8/spnego"
	"google.golang.org/grpc"

	protoutil "github.com/armadaproject/armada/internal/common/grpc/protoutils"
	"github.com/armadaproject/armada/internal/common/util"
)

// CreateGatewayHandler configures the gRPC API gateway
// by registering relevant handlers with the given mux.
func CreateGatewayHandler(
	grpcPort uint16,
	mux *http.ServeMux,
	apiBasePath string,
	ssl bool,
	corsAllowedOrigins []string,
	spec string,
	handlers ...func(ctx context.Context, mux *runtime.ServeMux, conn *grpc.ClientConn) error,
) (shutdown func()) {
	connectionCtx, cancelConnectionCtx := context.WithCancel(context.Background())

	grpcAddress := fmt.Sprintf(":%d", grpcPort)

	m := new(protoutil.JSONMarshaller)
	gw := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, m),
		runtime.WithOutgoingHeaderMatcher(func(key string) (string, bool) {
			if key == strings.ToLower(spnego.HTTPHeaderAuthResponse) {
				return spnego.HTTPHeaderAuthResponse, true
			}
			return fmt.Sprintf("%s%s", runtime.MetadataHeaderPrefix, key), true
		}))

	transportCreds := insecure.NewCredentials()
	if ssl {
		transportCreds = credentials.NewTLS(&tls.Config{InsecureSkipVerify: true})
	}

	conn, err := grpc.DialContext(connectionCtx, grpcAddress, grpc.WithTransportCredentials(transportCreds))
	if err != nil {
		panic(err)
	}

	// TODO this should return an error instead of calling panic
	for _, handler := range handlers {
		err = handler(connectionCtx, gw, conn)
		if err != nil {
			panic(err)
		}
	}

	mux.Handle(apiBasePath, allowCORS(gw, corsAllowedOrigins))
	mux.Handle(path.Join(apiBasePath, "swagger.json"), middleware.Spec(apiBasePath, []byte(spec), nil))

	return func() {
		cancelConnectionCtx()
		conn.Close()
	}
}

func allowCORS(h http.Handler, corsAllowedOrigins []string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if origin := r.Header.Get("Origin"); origin != "" && util.ContainsString(corsAllowedOrigins, origin) {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Credentials", "true")
			if r.Method == "OPTIONS" && r.Header.Get("Access-Control-Request-Method") != "" {
				preflightHandler(w)
				return
			}
		}
		h.ServeHTTP(w, r)
	})
}

func preflightHandler(w http.ResponseWriter) {
	headers := []string{"Content-Type", "Accept", "Authorization"}
	w.Header().Set("Access-Control-Allow-Headers", strings.Join(headers, ","))
	methods := []string{"GET", "HEAD", "POST", "PUT", "DELETE"}
	w.Header().Set("Access-Control-Allow-Methods", strings.Join(methods, ","))
}
