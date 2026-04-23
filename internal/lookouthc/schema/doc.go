// Package schema contains the consolidated initial schema for the lookouthc
// (hot-cold) Lookout database. The schema captures the final state of the
// original Lookout migrations 001-031, with the job table natively partitioned
// by LIST on state into job_active and job_terminated partitions.
//
// The new lookouthc stack runs on a separate database; the original Lookout
// database is unchanged. Migration of production environments uses a
// dual-instance rollover: the new stack accumulates history from Pulsar
// alongside the old one, then traffic is cut over via DNS/ingress.
package schema
