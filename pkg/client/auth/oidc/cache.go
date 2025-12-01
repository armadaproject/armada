package oidc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/99designs/keyring"
	"golang.org/x/oauth2"

	log "github.com/armadaproject/armada/internal/common/logging"
)

const (
	keyringServiceName = "armada-oidc"
)

type TokenCache struct {
	ring        keyring.Keyring
	providerUrl string
	clientId    string
}

type CachedToken struct {
	RefreshToken  string     `json:"refresh_token"`
	TokenType     string     `json:"token_type"`
	StoredAt      time.Time  `json:"stored_at"`
	RefreshExpiry *time.Time `json:"refresh_expiry,omitempty"`
}

func NewTokenCache(providerUrl, clientId string) (*TokenCache, error) {
	ring, err := keyring.Open(keyring.Config{
		ServiceName:                    keyringServiceName,
		AllowedBackends:                keyring.AvailableBackends(),
		KeychainTrustApplication:       true,
		KeychainSynchronizable:         false,
		KeychainAccessibleWhenUnlocked: true,
		KWalletAppID:                   "armada",
		KWalletFolder:                  "armada",
		LibSecretCollectionName:        "armada",
		WinCredPrefix:                  "armada",
	})
	if err != nil {
		log.Debug("No secure keyring backend available, token caching disabled")
		return nil, fmt.Errorf("no secure keyring backend available: %w", err)
	}

	return &TokenCache{
		ring:        ring,
		providerUrl: providerUrl,
		clientId:    clientId,
	}, nil
}

func (tc *TokenCache) getKey() string {
	return fmt.Sprintf("%s:%s", tc.providerUrl, tc.clientId)
}

func (tc *TokenCache) GetCachedRefreshToken() (string, error) {
	if tc == nil || tc.ring == nil {
		return "", errors.New("token cache not initialized")
	}

	key := tc.getKey()
	item, err := tc.ring.Get(key)
	if err != nil {
		if errors.Is(err, keyring.ErrKeyNotFound) {
			return "", nil // No cached token
		}
		return "", fmt.Errorf("failed to get token from keyring: %w", err)
	}

	var cached CachedToken
	if err := json.Unmarshal(item.Data, &cached); err != nil {
		return "", fmt.Errorf("failed to unmarshal cached token: %w", err)
	}

	if cached.RefreshExpiry != nil && time.Now().After(*cached.RefreshExpiry) {
		_ = tc.DeleteToken()
		return "", nil
	}

	return cached.RefreshToken, nil
}

func (tc *TokenCache) SaveRefreshToken(refreshToken string) error {
	if tc == nil || tc.ring == nil {
		return errors.New("token cache not initialized")
	}

	if refreshToken == "" {
		return errors.New("refresh token is empty")
	}

	key := tc.getKey()
	cached := CachedToken{
		RefreshToken: refreshToken,
		TokenType:    "Bearer",
		StoredAt:     time.Now(),
	}

	data, err := json.Marshal(cached)
	if err != nil {
		return fmt.Errorf("failed to marshal token: %w", err)
	}

	item := keyring.Item{
		Key:         key,
		Data:        data,
		Label:       fmt.Sprintf("Armada OIDC Refresh Token (%s)", tc.clientId),
		Description: fmt.Sprintf("OAuth2 refresh token for %s", tc.providerUrl),
	}

	if err := tc.ring.Set(item); err != nil {
		return fmt.Errorf("failed to save refresh token to keyring: %w", err)
	}

	return nil
}

func (tc *TokenCache) DeleteToken() error {
	if tc == nil || tc.ring == nil {
		return errors.New("token cache not initialized")
	}

	if err := tc.ring.Remove(tc.getKey()); err != nil && !errors.Is(err, keyring.ErrKeyNotFound) {
		return fmt.Errorf("failed to delete token from keyring: %w", err)
	}
	return nil
}

func RefreshTokenSecurely(ctx context.Context, config *oauth2.Config, refreshToken string, cache *TokenCache) (*oauth2.Token, error) {
	if refreshToken == "" {
		return nil, errors.New("no refresh token available")
	}

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
			if saveErr := cache.SaveRefreshToken(newToken.RefreshToken); saveErr != nil {
				log.WithError(saveErr).Error("Failed to save refreshed token to cache")
			}
		}
	} else {
		newToken.RefreshToken = refreshToken
	}

	return newToken, nil
}

func TryGetCachedToken(
	ctx context.Context,
	config *oauth2.Config,
	providerUrl string,
	clientId string,
	cacheEnabled bool,
) (*oauth2.Token, *TokenCache, error) {
	if !cacheEnabled {
		return nil, nil, nil
	}

	cache, err := NewTokenCache(providerUrl, clientId)
	if err != nil {
		log.Debug("Token cache unavailable, proceeding without caching")
		return nil, nil, nil
	}

	cachedRefreshToken, err := cache.GetCachedRefreshToken()
	if err != nil || cachedRefreshToken == "" {
		return nil, cache, nil
	}

	newToken, err := RefreshTokenSecurely(ctx, config, cachedRefreshToken, cache)
	if err != nil {
		_ = cache.DeleteToken()
		return nil, cache, nil
	}

	return newToken, cache, nil
}

func SaveTokenToCache(token *oauth2.Token, cache *TokenCache) {
	if cache == nil || token == nil || token.RefreshToken == "" {
		return
	}

	if err := cache.SaveRefreshToken(token.RefreshToken); err != nil {
		log.WithError(err).Debug("Failed to save token to cache")
	}
}
