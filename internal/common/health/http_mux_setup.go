package health

import (
	"net/http"
)

func SetupHttpMux(mux *http.ServeMux, checker Checker) {
	handler := NewHealthCheckHttpHandler(checker)
	mux.Handle("/health", handler)
}
