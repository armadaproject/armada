package common

import (
	"context"
	"encoding/base64"
)

type LoginCredentials struct {
	Username string
	Password string
}

func (c *LoginCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	auth := c.Username + ":" + c.Password
	return map[string]string{
		"authorization": "basic " + base64.StdEncoding.EncodeToString([]byte(auth)),
	}, nil
}

func (c *LoginCredentials) RequireTransportSecurity() bool {
	return false
}
