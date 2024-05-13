package mocks

// Mock implementations used by tests
//go:generate mockgen -destination=./mock_submitchecker.go -package=mocks "github.com/armadaproject/armada/internal/scheduler" SubmitScheduleChecker
//go:generate mockgen -destination=./mock_repository.go -package=mocks "github.com/armadaproject/armada/internal/armada/repository" QueueRepository,JobRepository
//go:generate mockgen -destination=./mock_deduplicator.go -package=mocks "github.com/armadaproject/armada/internal/armada/submit" Deduplicator
//go:generate mockgen -destination=./mock_authorizer.go -package=mocks "github.com/armadaproject/armada/internal/armada/server" ActionAuthorizer
