package common

import "context"

const UsernameField string = "username"
const PasswordField string = "password"

type LoginCredentials struct {
	Username string
	Password string
}

func (c *LoginCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{
		UsernameField: c.Username,
		PasswordField: c.Password,
	}, nil
}

func (c *LoginCredentials) RequireTransportSecurity() bool {
	return false
}
