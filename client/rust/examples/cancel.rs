// Cancel jobs in Armada.
//
// Auth (pick one):
//   ARMADA_USER / ARMADA_PASS  — HTTP Basic Auth (for clusters with basicAuth enabled)
//   ARMADA_TOKEN               — Bearer token   (for token-authenticated clusters)
//
// Other variables:
//   ARMADA_ENDPOINT  — gRPC endpoint, default: http://localhost:50051
//   ARMADA_QUEUE     — Queue name,    default: test
//   ARMADA_JOB_SET   — Job set ID,   default: rust-smoke-test
//   ARMADA_JOB_ID    — Job ID to cancel individually (optional)
//
// Cancel all queued/running jobs in a job set:
//   ARMADA_ENDPOINT=http://localhost:30002 \
//   ARMADA_USER=admin ARMADA_PASS=admin \
//   ARMADA_QUEUE=rust-test ARMADA_JOB_SET=my-job-set \
//   cargo run --manifest-path client/rust/Cargo.toml --example cancel

#[path = "common/mod.rs"]
mod common;

use armada_client::{
    ArmadaClient, JobCancelRequest, JobSetCancelRequest, JobSetFilter, JobState,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let endpoint =
        std::env::var("ARMADA_ENDPOINT").unwrap_or_else(|_| "http://localhost:50051".to_string());
    let queue = std::env::var("ARMADA_QUEUE").unwrap_or_else(|_| "test".to_string());
    let job_set_id =
        std::env::var("ARMADA_JOB_SET").unwrap_or_else(|_| "rust-smoke-test".to_string());
    let job_id = std::env::var("ARMADA_JOB_ID").ok();

    let client = ArmadaClient::connect(endpoint, common::auth_from_env()).await?;

    if let Some(id) = job_id {
        // Cancel a single job by ID.
        println!("Cancelling job '{id}' in queue '{queue}', job set '{job_set_id}'...");
        let result = client
            .cancel_jobs(JobCancelRequest {
                queue,
                job_set_id,
                job_ids: vec![id],
                reason: "cancelled via rust example".into(),
                ..Default::default()
            })
            .await?;
        println!("Cancelled {} job(s): {:?}", result.cancelled_ids.len(), result.cancelled_ids);
    } else {
        // Cancel all queued and running jobs in the job set.
        println!(
            "Cancelling queued/running jobs in queue '{queue}', job set '{job_set_id}'..."
        );
        client
            .cancel_job_set(JobSetCancelRequest {
                queue,
                job_set_id: job_set_id.clone(),
                filter: Some(JobSetFilter {
                    states: vec![JobState::Queued as i32, JobState::Running as i32],
                }),
                reason: "cancelled via rust example".into(),
            })
            .await?;
        println!("Cancellation requested for job set '{job_set_id}'.");
    }

    Ok(())
}
