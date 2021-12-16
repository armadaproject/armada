package health

import (
	"net/http"

	log "github.com/sirupsen/logrus"
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
		log.Info("Health check passed")
		w.WriteHeader(http.StatusNoContent)
	} else {
		log.Warnf("Health check failed: %v", err)
		w.WriteHeader(http.StatusServiceUnavailable)
		_, err = w.Write([]byte(err.Error()))
		if err != nil {
			log.Errorf("Failed to write health check response: %v", err)
		}
	}
}
