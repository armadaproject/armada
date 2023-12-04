package profiling

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
)

// SetupPprofHttpServer does two things:
//
//  1. Because importing "net/http/pprof" automatically binds profiling endpoints to http.DefaultServeMux,
//     this function replaces http.DefaultServeMux with a new mux. This to ensure profiling endpoints
//     are exposed on a separate mux available only from localhost.Hence, this function should be called
//     before adding any other endpoints to http.DefaultServeMux.
//  2. If port is non-nil, returns a http server serving net/http/pprof endpoints on localhost:port.
//     If port is nil, returns nil.
func SetupPprofHttpServer(port *uint16) *http.Server {
	pprofMux := http.DefaultServeMux
	http.DefaultServeMux = http.NewServeMux()
	return &http.Server{
		Addr:    fmt.Sprintf("localhost:%d", port),
		Handler: pprofMux,
	}
}
