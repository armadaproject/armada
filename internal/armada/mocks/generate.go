package mocks

// Mock implementations used by tests
//go:generate mockgen -destination=./mock_submitchecker.go -package=mocks "github.com/armadaproject/armada/internal/scheduler" SubmitScheduleChecker
//go:generate mockgen -destination=./mock_respository.go -package=mocks "github.com/armadaproject/armada/internal/armada/repository" QueueRepository
//go:generate mockgen -destination=./mock_deduplicator.go -package=mocks "github.com/armadaproject/armada/internal/armada/submit" Deduplicator,Publisher
//go:generate mockgen -destination=./mock_authorizer.go -package=mocks "github.com/armadaproject/armada/internal/armada/server" ActionAuthorizer
