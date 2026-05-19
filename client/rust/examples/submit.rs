// Submit a job to Armada.
//
// Auth (pick one):
//   ARMADA_USER / ARMADA_PASS  — HTTP Basic Auth (for clusters with basicAuth enabled)
//   ARMADA_TOKEN               — Bearer token   (for token-authenticated clusters)
//
// Other variables:
//   ARMADA_ENDPOINT  — gRPC endpoint, default: http://localhost:50051
//   ARMADA_QUEUE     — Queue name,    default: test
//   ARMADA_JOB_SET   — Job set ID,   default: rust-smoke-test
//
// Basic-auth example (quickstart secure cluster):
//   ARMADA_ENDPOINT=http://localhost:30002 \
//   ARMADA_USER=admin ARMADA_PASS=admin \
//   ARMADA_QUEUE=rust-test ARMADA_JOB_SET=my-job-set \
//   cargo run --manifest-path client/rust/Cargo.toml --example submit

#[path = "common/mod.rs"]
mod common;

use armada_client::k8s::io::api::core::v1::{Container, PodSpec, ResourceRequirements};
use armada_client::k8s::io::apimachinery::pkg::api::resource::Quantity;
use armada_client::{ArmadaClient, JobRequestItemBuilder, JobSubmitRequest};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let endpoint =
        std::env::var("ARMADA_ENDPOINT").unwrap_or_else(|_| "http://localhost:50051".to_string());
    let queue = std::env::var("ARMADA_QUEUE").unwrap_or_else(|_| "test".to_string());
    let job_set_id =
        std::env::var("ARMADA_JOB_SET").unwrap_or_else(|_| "rust-smoke-test".to_string());

    // Clone before move into connect so we can print it in the watch hint below.
    let endpoint_hint = endpoint.clone();
    let client = ArmadaClient::connect(endpoint, common::auth_from_env()).await?;

    // k8s proto2 fields are Option<T> in generated Rust
    let cpu = Quantity {
        string: Some("100m".to_string()),
    };
    let memory = Quantity {
        string: Some("64Mi".to_string()),
    };
    let container = Container {
        name: Some("main".to_string()),
        image: Some("busybox:latest".to_string()),
        command: vec!["sh".to_string()],
        args: vec!["-c".to_string(), "echo hello && sleep 5".to_string()],
        resources: Some(ResourceRequirements {
            requests: [
                ("cpu".to_string(), cpu.clone()),
                ("memory".to_string(), memory.clone()),
            ]
            .into_iter()
            .collect(),
            limits: [("cpu".to_string(), cpu), ("memory".to_string(), memory)]
                .into_iter()
                .collect(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let pod_spec = PodSpec {
        containers: vec![container],
        ..Default::default()
    };

    let item = JobRequestItemBuilder::new()
        .namespace("default")
        .priority(1.0)
        .label("app", "rust-smoke-test")
        .pod_spec(pod_spec)
        .build();

    let request = JobSubmitRequest {
        queue: queue.clone(),
        job_set_id: job_set_id.clone(),
        job_request_items: vec![item],
    };

    println!("Submitting job to queue '{queue}', job set '{job_set_id}'...");
    let response = client.submit(request).await?;
    println!("Submitted {} job(s):", response.job_response_items.len());
    for item in &response.job_response_items {
        if item.error.is_empty() {
            println!("  job_id={}", item.job_id);
        } else {
            println!("  ERROR: {}", item.error);
        }
    }

    println!("\nTo watch this job set:");
    println!(
        "  ARMADA_ENDPOINT={endpoint_hint} ARMADA_QUEUE={queue} ARMADA_JOB_SET={job_set_id} \\"
    );
    println!("  cargo run --manifest-path client/rust/Cargo.toml --example watch");

    Ok(())
}
