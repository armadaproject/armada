package authorization

import "context"

type AnonymousAuthService struct{}

func (AnonymousAuthService) Authenticate(ctx context.Context) (Principal, error) {
	return NewStaticPrincipal("anonymous", []string{}), nil
}
