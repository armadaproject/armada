package scheduler

import (
	"github.com/google/uuid"
)

type LeaderToken struct {
	leader bool
	id     uuid.UUID
}

func NewFollowerToken() LeaderToken {
	return LeaderToken{
		leader: false,
		id:     uuid.New(),
	}
}

func NewLeaderToken() LeaderToken {
	return LeaderToken{
		leader: true,
		id:     uuid.New(),
	}
}

type LeaderController interface {
	GetToken() LeaderToken
	ValidateToken(tok LeaderToken) bool
}

type StandaloneLeaderController struct {
	token LeaderToken
}

func NewStandaloneLeaderController() *StandaloneLeaderController {
	return &StandaloneLeaderController{
		token: NewLeaderToken(),
	}
}

func (lc *StandaloneLeaderController) GetToken() LeaderToken {
	return lc.token
}

func (lc *StandaloneLeaderController) ValidateToken(tok LeaderToken) bool {
	if tok.leader {
		return lc.token.id == tok.id
	}
	return false
}
