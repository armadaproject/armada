package mocks

// Mock implementations used by tests
//go:generate mockgen -destination=./mock_repository.go -package=mocks "github.com/armadaproject/armada/internal/armada/queue" QueueRepository
//go:generate mockgen -destination=./mock_deduplicator.go -package=mocks "github.com/armadaproject/armada/internal/armada/submit" Deduplicator,Publisher
//go:generate mockgen -destination=./mock_authorizer.go -package=mocks "github.com/armadaproject/armada/internal/armada/server" ActionAuthorizer
