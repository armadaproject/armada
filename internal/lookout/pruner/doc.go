// Package pruner contains the lookout database pruner.
//
// The pruner runs periodically and performs three tasks:
//
//  1. Reconciles "zombie" jobs whose state column is non-terminal but whose
//     latest run is in a terminal state. This addresses the residue of a now-
//     fixed ingester bug. Reconciliation is gated by a configurable grace
//     period to avoid racing in-flight state transitions and ingester lag.
//
//  2. Deletes terminal jobs (and their associated run, spec, and error rows)
//     that are older than a configurable lifetime, in batches.
//
//  3. Deletes job_deduplication rows older than a configurable lifetime.
//
// Step 1 runs first so that step 2's deletion sees correct terminal states.
package pruner
