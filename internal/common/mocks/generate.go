package mocks

// Mock implementations used by tests
//go:generate mockgen -destination=./mock_pulsar.go -package=mocks "github.com/apache/pulsar-client-go/pulsar" Client,Producer,Message
