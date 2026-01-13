package oidc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/zalando/go-keyring"
	"golang.org/x/oauth2"

	log "github.com/armadaproject/armada/internal/common/logging"
)

const (
	keyringServiceName = "armada-oidc"
)

var errCacheNotInitialized = errors.New("token cache not initialized")

type tokenCache struct {
	providerUrl string
	clientId    string
}

type cachedToken struct {
	RefreshToken string `json:"refresh_token"`
}

func newTokenCache(providerUrl, clientId string) (*tokenCache, error) {
	return &tokenCache{
		providerUrl: providerUrl,
		clientId:    clientId,
	}, nil
}

func (tc *tokenCache) getKey() string {
	return fmt.Sprintf("%s:%s", tc.providerUrl, tc.clientId)
}

func (tc *tokenCache) getCachedRefreshToken() (string, error) {
	if tc == nil {
		return "", errCacheNotInitialized
	}

	key := tc.getKey()
	data, err := keyring.Get(keyringServiceName, key)
	if err != nil {
		if errors.Is(err, keyring.ErrNotFound) {
			return "", nil
		}
		return "", fmt.Errorf("failed to get token from keyring: %w", err)
	}

	var cached cachedToken
	if err := json.Unmarshal([]byte(data), &cached); err != nil {
		return "", fmt.Errorf("failed to unmarshal cached token: %w", err)
	}

	return cached.RefreshToken, nil
}

func (tc *tokenCache) saveRefreshToken(refreshToken string) error {
	if tc == nil {
		return errCacheNotInitialized
	}

	if refreshToken == "" {
		return errors.New("refresh token is empty")
	}

	key := tc.getKey()
	cached := cachedToken{
		RefreshToken: refreshToken,
	}

	data, err := json.Marshal(cached)
	if err != nil {
		return fmt.Errorf("failed to marshal token: %w", err)
	}

	if err := keyring.Set(keyringServiceName, key, string(data)); err != nil {
		return fmt.Errorf("failed to save refresh token to keyring: %w", err)
	}

	return nil
}

func (tc *tokenCache) deleteToken() error {
	if tc == nil {
		return errCacheNotInitialized
	}

	if err := keyring.Delete(keyringServiceName, tc.getKey()); err != nil && !errors.Is(err, keyring.ErrNotFound) {
		return fmt.Errorf("failed to delete token from keyring: %w", err)
	}
	return nil
}

// refreshToken exchanges a refresh token for a new access token.
// If the provider returns a new refresh token (rotation), it updates the cache.
func refreshToken(ctx context.Context, config *oauth2.Config, refreshToken string, cache *tokenCache) (*oauth2.Token, error) {
	if refreshToken == "" {
		return nil, errors.New("no refresh token available")
	}

	// We construct a token with an expiry in the past to force oauth2.TokenSource
	// to perform a refresh. The oauth2 library only refreshes when the token is
	// expired; by setting Expiry to 1 hour ago, we guarantee the subsequent
	// tokenSource.Token() call will exchange our refresh token for a new access token.
	oldToken := &oauth2.Token{
		RefreshToken: refreshToken,
		TokenType:    "Bearer",
		Expiry:       time.Now().Add(-1 * time.Hour),
	}

	tokenSource := config.TokenSource(ctx, oldToken)

	newToken, err := tokenSource.Token()
	if err != nil {
		return nil, fmt.Errorf("failed to refresh token: %w", err)
	}

	if newToken.RefreshToken != "" {
		if cache != nil {
			if saveErr := cache.saveRefreshToken(newToken.RefreshToken); saveErr != nil {
				log.WithError(saveErr).Error("Failed to save refreshed token to cache")
			}
		}
	} else {
		newToken.RefreshToken = refreshToken
	}

	return newToken, nil
}

// tryGetCachedToken attempts to retrieve and refresh a cached token.
// Returns (nil, cache) if no valid cached token exists but caching is available.
func tryGetCachedToken(
	ctx context.Context,
	config *oauth2.Config,
	providerUrl string,
	clientId string,
	cacheEnabled bool,
) (*oauth2.Token, *tokenCache) {
	if !cacheEnabled {
		return nil, nil
	}

	cache, err := newTokenCache(providerUrl, clientId)
	if err != nil {
		log.Warn("Token cache unavailable, proceeding without caching")
		return nil, nil
	}

	cachedRefreshToken, err := cache.getCachedRefreshToken()
	if err != nil || cachedRefreshToken == "" {
		return nil, cache
	}

	newToken, err := refreshToken(ctx, config, cachedRefreshToken, cache)
	if err != nil {
		_ = cache.deleteToken()
		return nil, cache
	}

	return newToken, cache
}

func saveTokenToCache(token *oauth2.Token, cache *tokenCache) {
	if cache == nil || token == nil || token.RefreshToken == "" {
		return
	}

	if err := cache.saveRefreshToken(token.RefreshToken); err != nil {
		log.WithError(err).Error("Failed to save token to cache")
	}
}
