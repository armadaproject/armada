// Package pruner contains the lookout database pruner.
//
// The pruner runs periodically and performs three tasks:
//
//  1. Reconciles "zombie" jobs whose state column is non-terminal but whose
//     latest run is in a terminal state, or in a lease-returned/lease-expired
//     state whose expected follow-up event (requeue or failure) never
//     arrived. This addresses the residue of a now-fixed ingester bug, and of
//     job-level events lost by the ingester more generally. Reconciliation is
//     gated by two independently configurable grace periods to avoid racing
//     in-flight state transitions and ingester lag:
//     - a short one for the unconditionally terminal run states
//     - a longer one for lease-returned/lease-expired, which may still be
//     legitimately retried
//
//  2. Deletes terminal jobs (and their associated run, spec, and error rows)
//     that are older than a configurable lifetime, in batches.
//
//  3. Deletes job_deduplication rows older than a configurable lifetime.
//
// Step 1 runs first so that step 2's deletion sees correct terminal states.
package pruner
