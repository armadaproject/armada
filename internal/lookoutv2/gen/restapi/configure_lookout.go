// This file is safe to edit. Once it exists it will not be overwritten

package restapi

import (
	"crypto/tls"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime"
	"github.com/go-openapi/runtime/middleware"

	"github.com/armadaproject/armada/internal/common/serve"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/lookoutv2/configuration"
	"github.com/armadaproject/armada/internal/lookoutv2/gen/restapi/operations"
)

//go:generate swagger generate server --target ../../gen --name Lookout --spec ../../swagger.yaml --principal interface{} --exclude-main

var corsAllowedOrigins []string

func SetCorsAllowedOrigins(allowedOrigins []string) {
	corsAllowedOrigins = allowedOrigins
}

func configureFlags(api *operations.LookoutAPI) {
	// api.CommandLineOptionsGroups = []swag.CommandLineOptionsGroup{ ... }
}

func configureAPI(api *operations.LookoutAPI) http.Handler {
	// configure the api here
	api.ServeError = errors.ServeError

	// Set your custom logger if needed. Default one is log.Printf
	// Expected interface func(string, ...interface{})
	//
	// Example:
	// api.Logger = log.Printf

	api.UseSwaggerUI()
	// To continue using redoc as your UI, uncomment the following line
	// api.UseRedoc()

	api.JSONConsumer = runtime.JSONConsumer()

	api.JSONProducer = runtime.JSONProducer()

	if api.GetJobsHandler == nil {
		api.GetJobsHandler = operations.GetJobsHandlerFunc(func(params operations.GetJobsParams) middleware.Responder {
			return middleware.NotImplemented("operation operations.GetJobs has not yet been implemented")
		})
	}
	if api.GroupJobsHandler == nil {
		api.GroupJobsHandler = operations.GroupJobsHandlerFunc(func(params operations.GroupJobsParams) middleware.Responder {
			return middleware.NotImplemented("operation operations.GroupJobs has not yet been implemented")
		})
	}

	api.PreServerShutdown = func() {}

	api.ServerShutdown = func() {}

	return setupGlobalMiddleware(api.Serve(setupMiddlewares))
}

// The TLS configuration before HTTPS server starts.
func configureTLS(tlsConfig *tls.Config) {
	// Make all necessary changes to the TLS configuration here.
}

// As soon as server is initialized but not run yet, this function will be called.
// If you need to modify a config, store server instance to stop it individually later, this is the place.
// This function can be called multiple times, depending on the number of serving schemes.
// scheme value will be set accordingly: "http", "https" or "unix".
func configureServer(s *http.Server, scheme, addr string) {
}

// The middleware configuration is for the handler executors. These do not apply to the swagger.json document.
// The middleware executes after routing but before authentication, binding and validation.
func setupMiddlewares(handler http.Handler) http.Handler {
	return handler
}

var UIConfig configuration.UIConfig

// The middleware configuration happens before anything, this middleware also applies to serving the swagger.json document.
// So this is a good place to plug in a panic handling middleware, logging and metrics.
func setupGlobalMiddleware(apiHandler http.Handler) http.Handler {
	return allowCORS(uiHandler(apiHandler), corsAllowedOrigins)
}

func uiHandler(apiHandler http.Handler) http.Handler {
	mux := http.NewServeMux()

	mux.Handle("/", setCacheControl(http.FileServer(serve.CreateDirWithIndexFallback("./internal/lookout/ui/build"))))

	mux.HandleFunc("/config", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(UIConfig); err != nil {
			w.WriteHeader(500)
		}
	})

	mux.Handle("/api/", apiHandler)
	mux.Handle("/health", apiHandler)

	return mux
}

func setCacheControl(fileHandler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" || r.URL.Path == "/index.html" {
			// Because the version of index.html determines the version of the
			// JavaScript bundle, caching index.html would prevent users from
			// ever picking up new versions of the JavaScript bundle without
			// manually invalidating the cache.
			w.Header().Set("Cache-Control", "no-store")
		}
		fileHandler.ServeHTTP(w, r)
	})
}

func allowCORS(handler http.Handler, corsAllowedOrigins []string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if origin := r.Header.Get("Origin"); origin != "" && util.ContainsString(corsAllowedOrigins, origin) {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Credentials", "true")
			if r.Method == "OPTIONS" && r.Header.Get("Access-Control-Request-Method") != "" {
				preflightHandler(w)
				return
			}
		}
		handler.ServeHTTP(w, r)
	})
}

func preflightHandler(w http.ResponseWriter) {
	headers := []string{"Content-Type", "Accept", "Authorization"}
	w.Header().Set("Access-Control-Allow-Headers", strings.Join(headers, ","))
	methods := []string{"GET", "HEAD", "POST", "PUT", "DELETE"}
	w.Header().Set("Access-Control-Allow-Methods", strings.Join(methods, ","))
}
