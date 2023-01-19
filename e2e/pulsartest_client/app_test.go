package pulsartest_client

import (
	"testing"

	"github.com/stretchr/testify/assert"

	cfg "github.com/armadaproject/armada/internal/armada/configuration"
	pt "github.com/armadaproject/armada/internal/pulsartest"
)

func TestNew(t *testing.T) {
	// Basic success path
	pc := cfg.PulsarConfig{
		URL:               "pulsar://localhost:6650",
		JobsetEventsTopic: "events",
	}
	app, err := pt.New(pt.Params{Pulsar: pc}, "submit")
	assert.NoError(t, err)
	assert.NotNil(t, app)

	// Completely empty config
	pc = cfg.PulsarConfig{}
	app, err = pt.New(pt.Params{Pulsar: pc}, "submit")
	assert.Error(t, err)
	assert.Nil(t, app)

	// Missing topic name
	pc = cfg.PulsarConfig{
		URL: "pulsar://localhost:6650",
	}
	app, err = pt.New(pt.Params{Pulsar: pc}, "submit")
	assert.Error(t, err)
	assert.Nil(t, app)

	// Invalid compression type
	pc = cfg.PulsarConfig{
		URL:               "pulsar://localhost:6650",
		CompressionType:   "nocompression",
		JobsetEventsTopic: "events",
	}
	app, err = pt.New(pt.Params{Pulsar: pc}, "submit")
	assert.Error(t, err)
	assert.Nil(t, app)

	// Invalid compression level
	pc = cfg.PulsarConfig{
		URL:               "pulsar://localhost:6650",
		CompressionLevel:  "veryCompressed",
		JobsetEventsTopic: "events",
	}
	app, err = pt.New(pt.Params{Pulsar: pc}, "submit")
	assert.Error(t, err)
	assert.Nil(t, app)

	// Invalid command type
	pc = cfg.PulsarConfig{
		URL:               "pulsar://localhost:6650",
		JobsetEventsTopic: "events",
	}
	app, err = pt.New(pt.Params{Pulsar: pc}, "observe")
	assert.Error(t, err)
	assert.Nil(t, app)

	// Nonexistent topic
	pc = cfg.PulsarConfig{
		URL:               "pulsar://localhost:6650",
		JobsetEventsTopic: "persistent://armada/armada/nonesuch",
	}
	app, err = pt.New(pt.Params{Pulsar: pc}, "submit")
	assert.Error(t, err)
	assert.Nil(t, app)
}
