// Watch a job set and print events as they arrive.
//
// Required environment variables:
//   ARMADA_ENDPOINT  — gRPC endpoint, e.g. "http://localhost:50051"
//   ARMADA_TOKEN     — Bearer token (may be empty for unauthenticated clusters)
//   ARMADA_QUEUE     — Queue name
//   ARMADA_JOB_SET   — Job set ID to watch
//
// Run:
//   ARMADA_ENDPOINT=http://localhost:30002 \
//   ARMADA_QUEUE=rust-test \
//   ARMADA_JOB_SET=my-job-set \
//   cargo run --manifest-path client/rust/Cargo.toml --example watch

use armada_client::{ArmadaClient, StaticTokenProvider};
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let endpoint =
        std::env::var("ARMADA_ENDPOINT").unwrap_or_else(|_| "http://localhost:50051".to_string());
    let token = std::env::var("ARMADA_TOKEN").unwrap_or_default();
    if token.is_empty() {
        eprintln!("Warning: ARMADA_TOKEN is not set — requests may be rejected by the server");
    }
    let queue = std::env::var("ARMADA_QUEUE").unwrap_or_else(|_| "test".to_string());
    let job_set_id =
        std::env::var("ARMADA_JOB_SET").unwrap_or_else(|_| "rust-smoke-test".to_string());

    let client = ArmadaClient::connect(endpoint, StaticTokenProvider::new(token)).await?;

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
