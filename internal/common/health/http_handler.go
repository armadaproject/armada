package health

import (
	"fmt"
	"log/slog"
	"net/http"
)

// TODO Doesn't need to exist. Just give a Checker directly.
type CheckHttpHandler struct {
	checker Checker
}

func NewCheckHttpHandler(checker Checker) *CheckHttpHandler {
	return &CheckHttpHandler{
		checker: checker,
	}
}

// TODO Is this really the way to do it? We could give a handler that is an anonymous function
// handle that encloses the relevant variables.
// In doing so, we don't need CheckHttpHandler, NewCheckHttpHandler.
func (h *CheckHttpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	err := h.checker.Check()
	if err == nil {
		slog.Info("Health check passed")
		w.WriteHeader(http.StatusNoContent)
	} else {
		slog.Warn(fmt.Sprintf("Health check failed: %v", err))
		w.WriteHeader(http.StatusServiceUnavailable)
		_, err = w.Write([]byte(err.Error()))
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to write health check response: %v", err))
		}
	}
}
