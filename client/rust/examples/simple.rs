use std::collections::HashMap;

use anyhow::Result;
use armada_client::{
    armada::{
        api::{JobSubmitRequestItem, JobSubmitRequestItemBuilder, Queue},
        k8s::io::{
            api::core::v1::{
                ContainerBuilder, PodSpecBuilder, ResourceRequirementsBuilder,
                SecurityContextBuilder,
            },
            apimachinery::pkg::api::resource::Quantity,
        },
    },
    ArmadaClient,
};
use tonic::transport::Channel;

fn create_dummy_job() -> Result<Vec<JobSubmitRequestItem>> {
    let resources = HashMap::from([
        (
            String::from("cpu"),
            Quantity {
                string: Some(String::from("120m")),
            },
        ),
        (
            String::from("memory"),
            Quantity {
                string: Some(String::from("120m")),
            },
        ),
    ]);

    let container = ContainerBuilder::default()
        .name(Some(String::from("container1")))
        .image(Some("index.docker.io/library/ubuntu:latest".to_string()))
        .args(vec![String::from("sleep"), String::from("10s")])
        .security_context(Some(
            SecurityContextBuilder::default()
                .run_as_user(Some(1000))
                .build()?,
        ))
        .resources(Some(
            ResourceRequirementsBuilder::default()
                .requests(resources.clone())
                .limits(resources.clone())
                .build()?,
        ))
        .build()?;

    let pod_spec = PodSpecBuilder::default()
        .containers(vec![container])
        .build()?;

    let jsr = JobSubmitRequestItemBuilder::default()
        .priority(1.0)
        .pod_specs(vec![pod_spec])
        .namespace(String::from("personal-anonymous"))
        .build()?;
    Ok(vec![jsr])
}

#[tokio::main]
async fn main() -> Result<()> {
    let disable_ssl = std::env::var("DISABLE_SSL").map_or(false, |_| true);
    let host = std::env::var("ARMADA_SERVER").unwrap_or(String::from("localhost"));
    let port: u32 = std::env::var("ARMADA_PORT").map_or(50051, |p| p.parse::<u32>().unwrap());

    let channel = if disable_ssl {
        Channel::from_shared(format!("https://{host}:{port}"))?
            .connect()
            .await?
    } else {
        Channel::from_shared(format!("http://{host}:{port}"))?
            .connect()
            .await?
    };
    let mut client = ArmadaClient::new(channel);
    let queue = format!("simple-queue-{}", uuid::Uuid::new_v4());
    let job_set_id = format!("simple-jobset-{}", uuid::Uuid::new_v4());

    let mut queue_req = Queue::default();
    queue_req.name = queue.clone();
    queue_req.priority_factor = 1.0;
    client.create_queue(queue_req).await?;
    println!("Created queue: {}", queue);

    let request_items = create_dummy_job();
    client
        .submit_jobs(&queue, &job_set_id, request_items?)
        .await?;

    println!("Submitted job set: {}", job_set_id);

    println!("Watch with: ");
    println!("go run ./cmd/armadactl/main.go watch {queue} {job_set_id}");

    Ok(())
}
