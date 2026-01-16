package oidc

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"

	openId "github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"

	log "github.com/armadaproject/armada/internal/common/logging"
)

type PKCEDetails struct {
	ProviderUrl string
	ClientId    string
	LocalPort   uint16
	Scopes      []string
}

func AuthenticatePkce(config PKCEDetails, cacheToken bool) (*TokenCredentials, error) {
	ctx := context.Background()

	provider, err := openId.NewProvider(ctx, config.ProviderUrl)
	if err != nil {
		return nil, err
	}

	portStr := strconv.Itoa(int(config.LocalPort))
	localUrl := "localhost:" + portStr

	oauth := oauth2.Config{
		ClientID:    config.ClientId,
		Endpoint:    provider.Endpoint(),
		RedirectURL: "http://" + localUrl + "/auth/callback",
		Scopes:      append(config.Scopes, openId.ScopeOpenID),
	}

	// Try to use cached refresh token if enabled
	token, cache := tryGetCachedToken(ctx, &oauth, config.ProviderUrl, config.ClientId, cacheToken)
	if token != nil {
		return &TokenCredentials{oauth.TokenSource(ctx, token)}, nil
	}

	// Perform interactive authentication if no valid cached token
	result := make(chan *oauth2.Token)
	errorResult := make(chan error)

	state := randomStringBase64() // xss protection
	challenge := randomStringBase64()
	challengeSum := sha256.Sum256([]byte(challenge))
	challengeSumEncoded := strings.Replace(base64.URLEncoding.EncodeToString(challengeSum[:]), "=", "", -1)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		loginUrl := oauth.AuthCodeURL(state,
			oauth2.SetAuthURLParam("code_challenge", challengeSumEncoded),
			oauth2.SetAuthURLParam("code_challenge_method", "S256"))
		http.Redirect(w, r, loginUrl, http.StatusFound)
	})

	http.HandleFunc("/auth/callback", func(w http.ResponseWriter, r *http.Request) {
		_, err := fmt.Fprint(w, "<h1>Please close this window.</h1>")
		if err != nil {
			errorResult <- err
			return
		}

		if r.URL.Query().Get("state") != state {
			errorResult <- errors.New("Wrong state!")
			return
		}

		authError := r.URL.Query().Get("error")
		if authError != "" {
			authErrorDesc := r.URL.Query().Get("error_description")
			errorResult <- fmt.Errorf("%s: %s", authError, authErrorDesc)
			return
		}

		token, err := oauth.Exchange(ctx, r.URL.Query().Get("code"), oauth2.SetAuthURLParam("code_verifier", challenge))
		if err != nil {
			errorResult <- err
			return
		}
		result <- token
	})

	listener, err := net.Listen("tcp", localUrl)
	if err != nil {
		panic(err)
	}

	server := &http.Server{}
	defer func() {
		if err := server.Shutdown(context.Background()); err != nil {
			log.WithStacktrace(err).Error("unable to shutdown server gracefully")
		}
		listener.Close()
	}()

	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.WithStacktrace(err).Error("unable to serve")
		}
	}()

	cmd, err := openBrowser("http://" + localUrl)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := cmd.Process.Kill(); err != nil {
			log.WithStacktrace(err).Error("unable to kill process")
		}
	}()

	select {
	case t := <-result:
		saveTokenToCache(t, cache)
		return &TokenCredentials{oauth.TokenSource(ctx, t)}, nil
	case e := <-errorResult:
		return nil, e
	}
}

func randomStringBase64() string {
	b := make([]byte, 32)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	return strings.Replace(base64.URLEncoding.EncodeToString(b), "=", "", -1)
}
