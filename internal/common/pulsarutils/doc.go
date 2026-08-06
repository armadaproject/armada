// Package pulsarutils provides shared helpers for constructing and using
// Pulsar clients, producers, and consumers.
//
// [NewPulsarClient] and [NewPulsarAdminClient] construct clients from
// [config.PulsarConfig]. [PulsarPublisher] proto-marshals and publishes
// [utils.ArmadaEvent] messages. [DeadLetterPublisher] publishes raw,
// sink-serialized payloads (with originating-topic/subscription/attempt
// metadata) to a dead-letter topic for messages an ingestion pipeline has
// given up retrying; see [internal/common/ingest].
package pulsarutils
