package health

import (
	"net/http"
)

func SetupHttpMux(mux *http.ServeMux, checker Checker) {
	handler := NewCheckHttpHandler(checker)
	mux.Handle("/health", handler)
}
