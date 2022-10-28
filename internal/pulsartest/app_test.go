package pulsartest

import (
	"testing"

	"github.com/stretchr/testify/assert"

	cfg "github.com/G-Research/armada/internal/armada/configuration"
)

func TestNew(t *testing.T) {
	// Basic success path
	pc := cfg.PulsarConfig{
		URL:               "pulsar://localhost:6650",
		JobsetEventsTopic: "persistent://armada/armada/events",
	}
	app, err := New(Params{Pulsar: pc}, "submit")
	assert.NoError(t, err)

	// Completely empty config
	pc = cfg.PulsarConfig{}
	app, err = New(Params{Pulsar: pc}, "submit")
	assert.Error(t, err)
	assert.Nil(t, app)

	// Missing topic name
	pc = cfg.PulsarConfig{
		URL: "pulsar://localhost:6650",
	}
	app, err = New(Params{Pulsar: pc}, "submit")
	assert.Error(t, err)
	assert.Nil(t, app)

	// Invalid compression type
	pc = cfg.PulsarConfig{
		URL:               "pulsar://localhost:6650",
		CompressionType:   "nocompression",
		JobsetEventsTopic: "persistent://armada/armada/events",
	}
	app, err = New(Params{Pulsar: pc}, "submit")
	assert.Error(t, err)
	assert.Nil(t, app)

	// Invalid compression level
	pc = cfg.PulsarConfig{
		URL:               "pulsar://localhost:6650",
		CompressionLevel:  "veryCompressed",
		JobsetEventsTopic: "persistent://armada/armada/events",
	}
	app, err = New(Params{Pulsar: pc}, "submit")
	assert.Error(t, err)
	assert.Nil(t, app)

	// Invalid command type
	pc = cfg.PulsarConfig{
		URL:               "pulsar://localhost:6650",
		JobsetEventsTopic: "persistent://armada/armada/events",
	}
	app, err = New(Params{Pulsar: pc}, "observe")
	assert.Error(t, err)
	assert.Nil(t, app)

	// Nonexistent topic
	pc = cfg.PulsarConfig{
		URL:               "pulsar://localhost:6650",
		JobsetEventsTopic: "persistent://armada/armada/nonesuch",
	}
	app, err = New(Params{Pulsar: pc}, "submit")
	assert.Error(t, err)
	assert.Nil(t, app)
}
