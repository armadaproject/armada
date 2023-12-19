package serve

import (
	"net/http"

	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/logging"
)

// ListenAndServe calls server.ListenAndServe().
// Additionally, it calls server.Shutdown() if ctx is cancelled.
func ListenAndServe(ctx *armadacontext.Context, server *http.Server) error {
	if server == nil {
		return nil
	}
	go func() {
		// Shutdown server on ctx done.
		<-ctx.Done()
		if err := server.Shutdown(ctx); err != nil {
			logging.WithStacktrace(ctx, err).Errorf("failed to shutdown server serving %s", server.Addr)
		}
	}()
	if err := server.ListenAndServe(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}
