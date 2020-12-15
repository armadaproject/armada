package grpc

import (
	"context"
	"fmt"
	"github.com/golang/glog"
	"net/http"
	"path"
	"strings"

	"github.com/go-openapi/runtime/middleware"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/jcmturner/gokrb5/v8/spnego"
	"google.golang.org/grpc"

	protoutil "github.com/G-Research/armada/internal/armada/protoutils"
	"github.com/G-Research/armada/internal/common"
)

func ServeGateway(
	port uint16,
	grpcPort uint16,
	spec string,
	handlers ...func(ctx context.Context, mux *runtime.ServeMux, conn *grpc.ClientConn) error) (shutdown func()) {

	mux, shutdownGateway := CreateGatewayHandler(grpcPort, "/", spec, handlers...)
	cancel := common.ServeHttp(port, mux)

	return func() {
		shutdownGateway()
		cancel()
	}
}

func CreateGatewayHandler(
	grpcPort uint16,
	apiBasePath string,
	spec string,
	handlers ...func(ctx context.Context, mux *runtime.ServeMux, conn *grpc.ClientConn) error) (handler *http.ServeMux, shutdown func()) {

	connectionCtx, cancelConnectionCtx := context.WithCancel(context.Background())

	grpcAddress := fmt.Sprintf(":%d", grpcPort)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", health)

	m := new(protoutil.JSONMarshaller)
	gw := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, m),
		runtime.WithOutgoingHeaderMatcher(func(key string) (string, bool) {
			if key == strings.ToLower(spnego.HTTPHeaderAuthResponse) {
				return spnego.HTTPHeaderAuthResponse, true
			}
			return fmt.Sprintf("%s%s", runtime.MetadataHeaderPrefix, key), true
		}))

	conn, err := grpc.DialContext(connectionCtx, grpcAddress, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}

	for _, handler := range handlers {
		err = handler(connectionCtx, gw, conn)
		if err != nil {
			panic(err)
		}
	}

	mux.Handle(apiBasePath, allowCORS(gw))
	mux.Handle(path.Join(apiBasePath, "swagger.json"), middleware.Spec(apiBasePath, []byte(spec), nil))

	return mux, func() {
		cancelConnectionCtx()
		conn.Close()
	}
}

func allowCORS(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if origin := r.Header.Get("Origin"); origin != "" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			if r.Method == "OPTIONS" && r.Header.Get("Access-Control-Request-Method") != "" {
				preflightHandler(w, r)
				return
			}
		}
		h.ServeHTTP(w, r)
	})
}

func preflightHandler(w http.ResponseWriter, r *http.Request) {
	headers := []string{"Content-Type", "Accept"}
	w.Header().Set("Access-Control-Allow-Headers", strings.Join(headers, ","))
	methods := []string{"GET", "HEAD", "POST", "PUT", "DELETE"}
	w.Header().Set("Access-Control-Allow-Methods", strings.Join(methods, ","))
	glog.Infof("preflight request for %s", r.URL.Path)
}

func health(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}
