use std::collections::HashMap;

use anyhow::Result;
use armada_client::{
    armada::{
        api::{JobSubmitRequestItem, Queue},
        k8s::io::{
            api::core::v1::{Container, PodSpec, ResourceRequirements, SecurityContext},
            apimachinery::pkg::api::resource::Quantity,
        },
    },
    ArmadaClient,
};
use tonic::transport::Channel;

fn create_dummy_job() -> Vec<JobSubmitRequestItem> {
    let pod_spec = PodSpec {
        volumes: vec![],
        init_containers: vec![],
        containers: vec![Container {
            name: Some("container1".to_string()),
            image: Some("index.docker.io/library/ubuntu:latest".to_string()),
            args: vec!["sleep".to_string(), "10s".to_string()],
            security_context: Some(SecurityContext {
                run_as_user: Some(1000),
                capabilities: None,
                privileged: None,
                se_linux_options: None,
                windows_options: None,
                run_as_group: None,
                run_as_non_root: None,
                read_only_root_filesystem: None,
                allow_privilege_escalation: None,
                proc_mount: None,
                seccomp_profile: None,
            }),
            resources: Some(ResourceRequirements {
                requests: {
                    let mut map = HashMap::new();
                    map.insert(
                        "cpu".to_string(),
                        Quantity {
                            string: Some("120m".to_string()),
                        },
                    );
                    map.insert(
                        "memory".to_string(),
                        Quantity {
                            string: Some("510Mi".to_string()),
                        },
                    );
                    map
                },
                limits: {
                    let mut map = HashMap::new();
                    map.insert(
                        "cpu".to_string(),
                        Quantity {
                            string: Some("120m".to_string()),
                        },
                    );
                    map.insert(
                        "memory".to_string(),
                        Quantity {
                            string: Some("510Mi".to_string()),
                        },
                    );
                    map
                },
            }),
            command: vec![],
            working_dir: None,
            ports: vec![],
            env_from: vec![],
            env: vec![],
            volume_mounts: vec![],
            volume_devices: vec![],
            liveness_probe: None,
            readiness_probe: None,
            startup_probe: None,
            lifecycle: None,
            termination_message_path: None,
            termination_message_policy: None,
            image_pull_policy: None,
            stdin: None,
            stdin_once: None,
            tty: None,
        }],
        ephemeral_containers: vec![],
        restart_policy: None,
        termination_grace_period_seconds: None,
        active_deadline_seconds: None,
        dns_policy: None,
        dns_config: None,
        node_selector: HashMap::new(),
        service_account: None,
        service_account_name: None,
        node_name: None,
        host_network: None,
        host_pid: None,
        host_ipc: None,
        share_process_namespace: None,
        security_context: None,
        image_pull_secrets: vec![],
        hostname: None,
        subdomain: None,
        affinity: None,
        scheduler_name: None,
        tolerations: vec![],
        host_aliases: vec![],
        priority: None,
        priority_class_name: None,
        readiness_gates: vec![],
        runtime_class_name: None,
        enable_service_links: None,
        preemption_policy: None,
        overhead: HashMap::new(),
        topology_spread_constraints: vec![],
        set_hostname_as_fqdn: None,
        automount_service_account_token: None,
    };
    vec![JobSubmitRequestItem {
        priority: 1.,
        pod_specs: vec![pod_spec],
        namespace: "personal-anonymous".to_string(),
        client_id: String::new(),
        labels: HashMap::new(),
        annotations: HashMap::new(),
        required_node_labels: HashMap::new(),
        pod_spec: None,
        ingress: vec![],
        services: vec![],
        scheduler: String::new(),
    }]
}

#[tokio::main]
async fn main() -> Result<()> {
    let channel = Channel::from_static("http://[::1]:50051").connect().await?;
    let mut client = ArmadaClient::new(channel);
    let queue = format!("simple-queue-{}", uuid::Uuid::new_v4());
    let job_set_id = format!("simple-jobset-{}", uuid::Uuid::new_v4());

    let queue_req = Queue {
        name: queue.clone(),
        priority_factor: 1.0,
        user_owners: vec![],
        group_owners: vec![],
        resource_limits: HashMap::new(),
        permissions: vec![],
    };
    client.create_queue(queue_req).await?;
    println!("Created queue: {}", queue);

    let request_items = create_dummy_job();
    client.submit_jobs(queue, job_set_id.clone(), request_items).await?;

    println!("Submitted job set: {}", job_set_id);

    Ok(())
}
