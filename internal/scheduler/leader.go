package scheduler

import (
	"github.com/google/uuid"
)

// LeaderController is an interface to be implemented by structs that control which scheduler is leader
type LeaderController interface {
	// GetToken returns a LeaderToken which allows you to determine if you are leader or not
	GetToken() LeaderToken
	// ValidateToken allows a caller to determine whether a previously obtained token is still valid.
	// Returns true if the token is a leader and false otherwise
	ValidateToken(tok LeaderToken) bool
}

// StandaloneLeaderController returns a token that always indicates you are leader
// This can be used when only a single instance of the scheduler is  needed
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

// LeaderToken is a token handed out to schedulers which they can use to determine if they are leader
type LeaderToken struct {
	leader bool
	id     uuid.UUID
}

// InvalidLeaderToken returns a LeaderToken which indicates the scheduler is not leader
func InvalidLeaderToken() LeaderToken {
	return LeaderToken{
		leader: false,
		id:     uuid.New(),
	}
}

// NewLeaderToken returns a LeaderToken which indicates the scheduler is leader
func NewLeaderToken() LeaderToken {
	return LeaderToken{
		leader: true,
		id:     uuid.New(),
	}
}
