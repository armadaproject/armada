package auth

import "context"

const AnonymousAuthServiceName = "Anonymous"

type AnonymousAuthService struct{}

// Default principal used if no principal can be found in a context.
var anonymousPrincipal = NewStaticPrincipal("anonymous", AnonymousAuthServiceName, []string{})

func (AnonymousAuthService) Authenticate(ctx context.Context, authHeader string) (Principal, error) {
	return anonymousPrincipal, nil
}
