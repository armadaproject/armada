package authorization

import "context"

type AnonymousGodAuthService struct{}

func (AnonymousGodAuthService) Authenticate(ctx context.Context) (Principal, error) {
	return &godPrincipal{}, nil
}

type godPrincipal struct {
}

func (godPrincipal) GetName() string             { return "anonymous" }
func (godPrincipal) IsInGroup(group string) bool { return true }
func (godPrincipal) HasScope(scope string) bool  { return true }
