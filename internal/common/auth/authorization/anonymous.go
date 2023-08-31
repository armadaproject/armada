package authorization

import "context"

type AnonymousAuthService struct{}

func (authService *AnonymousAuthService) Name() string {
	return "Anonymous"
}

func (AnonymousAuthService) Authenticate(ctx context.Context) (Principal, error) {
	return anonymousPrincipal, nil
}
