use anyhow::Result;
use armada_client::{
    armada::{
        api::{EventStreamMessage, JobState, JobSubmitRequestItem, Queue},
        k8s::io::{
            api::core::v1::{Container, PodSpec, ResourceRequirements, SecurityContext},
            apimachinery::pkg::api::resource::Quantity,
        },
    },
    ArmadaClient,
};
use tokio::time::{sleep, Duration};
use tonic::transport::Channel;

fn create_dummy_job() -> Vec<JobSubmitRequestItem> {
    let mut pod_spec = PodSpec::default();
    let mut container = Container::default();
    container.name = Some(String::from("container1"));
    container.image = Some("index.docker.io/library/ubuntu:latest".to_string());
    container.args = vec![String::from("sleep"), String::from("10s")];
    container.security_context = Some({
        let mut sec_ctx = SecurityContext::default();
        sec_ctx.run_as_user = Some(1000);
        sec_ctx
    });
    container.resources = Some({
        let mut reqs = ResourceRequirements::default();
        reqs.requests.insert(
            String::from("cpu"),
            Quantity {
                string: Some(String::from("120m")),
            },
        );
        reqs.requests.insert(
            String::from("memory"),
            Quantity {
                string: Some(String::from("510Mi")),
            },
        );

        reqs.limits.insert(
            String::from("cpu"),
            Quantity {
                string: Some(String::from("120m")),
            },
        );
        reqs.limits.insert(
            String::from("memory"),
            Quantity {
                string: Some(String::from("510Mi")),
            },
        );
        reqs
    });
    pod_spec.containers = vec![container];

    let mut jsr = JobSubmitRequestItem::default();
    jsr.priority = 1.0;
    jsr.pod_specs = vec![pod_spec];
    jsr.namespace = "personal-anonymous".to_string();
    vec![jsr]
}

async fn create_queue(client: &mut ArmadaClient, queue: &str) -> Result<()> {
    let mut queue_req = Queue::default();
    queue_req.name = queue.to_string();
    queue_req.priority_factor = 1.0;
    client.create_queue(queue_req).await?;
    println!("Created queue: {}", queue);
    Ok(())
}

async fn wait_for_job_event(
    client: &mut ArmadaClient,
    event_stream: &mut tonic::Streaming<EventStreamMessage>,
    job_set_id: &str,
) -> Result<bool> {
    while let Some(message) = event_stream.message().await? {}
    todo!()
}

async fn create_and_monitor_jobs(
    client: &mut ArmadaClient,
    queue: &str,
    job_set_id: &str,
) -> Result<()> {
    let request_items = create_dummy_job();

    let resp = client.submit_jobs(queue, job_set_id, request_items).await?;

    let job_id = &resp
        .get_ref()
        .job_response_items
        .get(0)
        .expect("Failed to get the job response")
        .job_id;

    client
        .reprioritize_jobs(2.0, vec![], queue, job_set_id)
        .await?;

    sleep(Duration::from_millis(2000)).await;

    let event_stream = client.get_job_events_stream(queue, "", job_set_id).await?;
    todo!()

    println!("Submitted job set: {}", job_set_id);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let disable_ssl = std::env::var("DISABLE_SSL").map_or(false, |_| true);
    let host = std::env::var("ARMADA_SERVER").unwrap_or(String::from("localhost"));
    let port: u32 = std::env::var("ARMADA_PORT").map_or(50051, |p| p.parse::<u32>().unwrap());

    let channel = if disable_ssl {
        Channel::from_shared(format!("http://{host}:{port}"))?
            .connect()
            .await?
    } else {
        Channel::from_shared(format!("https://{host}:{port}"))?
            .connect()
            .await?
    };
    let mut client = ArmadaClient::new(channel);
    let queue = format!("simple-queue-{}", uuid::Uuid::new_v4());
    let job_set_id = format!("simple-jobset-{}", uuid::Uuid::new_v4());

    create_queue(&mut client, &queue).await?;
    create_and_monitor_jobs(&mut client, &queue, &job_set_id).await?;
    Ok(())
}
