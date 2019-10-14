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
	"os/exec"
	"runtime"
	"strconv"
	"strings"

	openId "github.com/coreos/go-oidc"
	"golang.org/x/oauth2"

	"github.com/G-Research/k8s-batch/internal/client/domain"
)

type TokenCredentials struct {
	tokenSource oauth2.TokenSource
}

func (c *TokenCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	jwt, err := GetJWT(c.tokenSource)
	if err != nil {
		return nil, err
	}
	return map[string]string{
		"authorization": "Bearer " + jwt,
	}, nil
}

func (c *TokenCredentials) RequireTransportSecurity() bool {
	return false
}

func GetJWT(source oauth2.TokenSource) (string, error) {
	t, e := source.Token()
	if e != nil {
		return "", e
	}
	jwt, ok := t.Extra("id_token").(string)
	if !ok || jwt == "" {
		return "", errors.New("missing ID Token")
	}
	return jwt, nil
}

func AuthenticatePkce(config domain.OpenIdConnectClientDetails) (*TokenCredentials, error) {

	ctx := context.Background()
	result := make(chan *oauth2.Token)
	errorResult := make(chan error)

	provider, err := openId.NewProvider(ctx, config.ProviderUrl)
	if err != nil {
		panic(err)
	}

	localUrl := "localhost:" + strconv.Itoa(int(config.LocalPort))

	oauth := oauth2.Config{
		ClientID:    config.ClientId,
		Endpoint:    provider.Endpoint(),
		RedirectURL: "http://" + localUrl + "/auth/callback",
		Scopes:      append(config.Scopes, openId.ScopeOpenID),
	}

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
	defer listener.Close()

	server := &http.Server{}

	go server.Serve(listener)

	cmd, err := openbrowser("http://" + localUrl)
	defer cmd.Process.Kill()

	if err != nil {
		return nil, err
	}

	select {
	case t := <-result:
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

func openbrowser(url string) (*exec.Cmd, error) {
	cmd := browserCommand(url)
	return cmd, cmd.Start()
}

func browserCommand(url string) *exec.Cmd {
	switch runtime.GOOS {
	case "linux":
		return exec.Command("xdg-open", url)
	case "windows":
		return exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	case "darwin":
		return exec.Command("open", url)
	}
	panic(fmt.Errorf("unsupported platform"))
}
