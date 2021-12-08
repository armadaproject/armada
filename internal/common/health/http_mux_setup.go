package health

import (
	"net/http"
)

// TODO This function doesn't need to exist.
// Just return a callback function and let the caller decide what to do
// (e.g., register it to a mux).
func SetupHttpMux(mux *http.ServeMux, checker Checker) {
	handler := NewCheckHttpHandler(checker)
	mux.Handle("/health", handler)
}
