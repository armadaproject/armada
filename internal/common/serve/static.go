package serve

import (
	"net/http"

	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/logging"
)

type dirWithIndexFallback struct {
	dir http.Dir
}

func CreateDirWithIndexFallback(path string) http.FileSystem {
	return dirWithIndexFallback{http.Dir(path)}
}

func (d dirWithIndexFallback) Open(name string) (http.File, error) {
	file, err := d.dir.Open(name)
	if err != nil {
		return d.dir.Open("index.html")
	}
	return file, err
}

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
