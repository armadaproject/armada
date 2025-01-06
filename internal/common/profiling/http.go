package profiling

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"

	"golang.org/x/sync/errgroup"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/auth"
	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/common/profiling/configuration"
	"github.com/armadaproject/armada/internal/common/serve"
)

func SetupPprof(config *configuration.ProfilingConfig, ctx *armadacontext.Context, g *errgroup.Group) error {
	if config == nil {
		log.Infof("Pprof server not configured, skipping")
		return nil
	}

	log.Infof("Setting up pprof server on port %d", config.Port)
	if config.Auth == nil {
		log.Errorf("Pprof server auth not configured, will not set up pprof")
		return nil
	}

	authServices, err := auth.ConfigureAuth(*config.Auth)
	if err != nil {
		return fmt.Errorf("error configuring pprof auth :%v", err)
	}

	authenticationFunc := auth.CreateHttpMiddlewareAuthFunction(auth.NewMultiAuthService(authServices))

	authorizer := auth.NewAuthorizer(
		auth.NewPrincipalPermissionChecker(
			config.Auth.PermissionGroupMapping,
			config.Auth.PermissionScopeMapping,
			config.Auth.PermissionClaimMapping,
		),
	)

	pprofServer := setupPprofHttpServerWithAuth(config.Port, func(w http.ResponseWriter, r *http.Request) (context.Context, error) {
		ctx, err := authenticationFunc(w, r)
		if err != nil {
			return nil, err
		}

		return ctx, authorizer.AuthorizeAction(armadacontext.FromGrpcCtx(ctx), "pprof")
	})

	serveFunc := func() error {
		if err := serve.ListenAndServe(ctx, pprofServer); err != nil {
			ctx.Logger().WithStacktrace(err).Error("pprof server failure")
		}
		return err
	}

	if g != nil {
		g.Go(serveFunc)
	} else {
		go func() {
			_ = serveFunc()
		}()
	}
	return nil
}

// SetupPprofHttpServer does two things:
//
//  1. Because importing "net/http/pprof" automatically binds profiling endpoints to http.DefaultServeMux,
//     this function replaces http.DefaultServeMux with a new mux. This to ensure profiling endpoints
//     are exposed on a separate mux available only from localhost.Hence, this function should be called
//     before adding any other endpoints to http.DefaultServeMux.
//  2. Returns a http server serving net/http/pprof endpoints on localhost:port.
func setupPprofHttpServerWithAuth(port uint16, authFunc func(w http.ResponseWriter, r *http.Request) (context.Context, error)) *http.Server {
	pprofMux := http.DefaultServeMux
	http.DefaultServeMux = http.NewServeMux()

	authInterceptor := AuthInterceptor{
		underlying: pprofMux,
		authFunc:   authFunc,
	}

	return &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: authInterceptor,
	}
}

type AuthInterceptor struct {
	underlying http.Handler
	authFunc   func(w http.ResponseWriter, r *http.Request) (context.Context, error)
}

func (i AuthInterceptor) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx, err := i.authFunc(w, r)
	if err != nil {
		log.Errorf("Pprof auth failed: %v", err)
		return
	}
	principal := auth.GetPrincipal(ctx)
	log.Infof("Pprof auth succeeded (method %s, principal %s)", principal.GetAuthMethod(), principal.GetName())
	i.underlying.ServeHTTP(w, r)
}
