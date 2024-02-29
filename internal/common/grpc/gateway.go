package grpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"path"
	"strings"

	"github.com/go-openapi/runtime/middleware"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	protoutil "github.com/armadaproject/armada/internal/common/grpc/protoutils"
	"github.com/armadaproject/armada/internal/common/util"
)

// CreateGatewayHandler configures the gRPC API gateway
// by registering relevant handlers with the given mux.
func CreateGatewayHandler(
	grpcPort uint16,
	mux *http.ServeMux,
	apiBasePath string,
	stripPrefix bool,
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

	if stripPrefix {
		prefixToStrip := strings.TrimSuffix(apiBasePath, "/")
		mux.Handle(apiBasePath, http.StripPrefix(prefixToStrip, logRestRequests(allowCORS(gw, corsAllowedOrigins))))
	} else {
		mux.Handle(apiBasePath, logRestRequests(allowCORS(gw, corsAllowedOrigins)))
	}
	mux.Handle(path.Join(apiBasePath, "swagger.json"), middleware.Spec(apiBasePath, []byte(spec), nil))

	return func() {
		cancelConnectionCtx()
		conn.Close()
	}
}

func logRestRequests(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Infof("Received REST request performing %s %s", r.Method, r.URL.Path)
		h.ServeHTTP(w, r)
	})
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
