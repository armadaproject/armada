package mocks

// Mock implementations used by tests
//go:generate mockgen -destination=./mock_deduplicator.go -package=mocks "github.com/armadaproject/armada/internal/server/submit" Deduplicator
//go:generate mockgen -destination=./mock_authorizer.go -package=mocks "github.com/armadaproject/armada/internal/common/auth" ActionAuthorizer
//go:generate mockgen -destination=./mock_repository.go -package=mocks "github.com/armadaproject/armada/internal/server/queue" QueueRepository
