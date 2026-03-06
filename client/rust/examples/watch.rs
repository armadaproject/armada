// Watch a job set and print events as they arrive.
//
// Auth (pick one):
//   ARMADA_USER / ARMADA_PASS  — HTTP Basic Auth (for clusters with basicAuth enabled)
//   ARMADA_TOKEN               — Bearer token   (for token-authenticated clusters)
//
// Other variables:
//   ARMADA_ENDPOINT  — gRPC endpoint, default: http://localhost:50051
//   ARMADA_QUEUE     — Queue name,    default: test
//   ARMADA_JOB_SET   — Job set ID to watch
//
// Basic-auth example (quickstart secure cluster):
//   ARMADA_ENDPOINT=http://localhost:30002 \
//   ARMADA_USER=admin ARMADA_PASS=admin \
//   ARMADA_QUEUE=rust-test ARMADA_JOB_SET=my-job-set \
//   cargo run --manifest-path client/rust/Cargo.toml --example watch

#[path = "common/mod.rs"]
mod common;

use armada_client::ArmadaClient;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let endpoint =
        std::env::var("ARMADA_ENDPOINT").unwrap_or_else(|_| "http://localhost:50051".to_string());
    let queue = std::env::var("ARMADA_QUEUE").unwrap_or_else(|_| "test".to_string());
    let job_set_id =
        std::env::var("ARMADA_JOB_SET").unwrap_or_else(|_| "rust-smoke-test".to_string());

    let client = ArmadaClient::connect(endpoint, common::auth_from_env()).await?;

    println!("Watching job set '{job_set_id}' on queue '{queue}'...");
    let mut stream = client.watch(&queue, &job_set_id, None).await?;
    while let Some(event) = stream.next().await {
        match event {
            Ok(msg) => {
                println!("  event id={} message={:?}", msg.id, msg.message);
            }
            Err(e) => {
                eprintln!("  stream error: {e}");
                break;
            }
        }
    }

    Ok(())
}
