package authorization

import "github.com/armadaproject/armada/internal/common/context"

type AnonymousAuthService struct{}

func (authService *AnonymousAuthService) Name() string {
	return "Anonymous"
}

func (AnonymousAuthService) Authenticate(ctx *context.ArmadaContext) (Principal, error) {
	return anonymousPrincipal, nil
}
