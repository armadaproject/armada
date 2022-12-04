use tonic::transport::Channel;

use crate::armada::api::{
    event_client::EventClient, submit_client::SubmitClient, BatchQueueCreateResponse,
    BatchQueueUpdateResponse, CancellationResult, EventStreamMessage, HealthCheckResponse,
    JobCancelRequest, JobReprioritizeRequest, JobReprioritizeResponse, JobSetCancelRequest,
    JobSetFilter, JobSetRequest, JobState, JobSubmitRequest, JobSubmitRequestItem,
    JobSubmitResponse, Queue, QueueDeleteRequest, QueueGetRequest, QueueInfo, QueueInfoRequest,
    QueueList,
};

pub struct ArmadaClient {
    event_client: EventClient<tonic::transport::Channel>,
    submit_client: SubmitClient<tonic::transport::Channel>,
}

impl ArmadaClient {
    pub fn new(channel: Channel) -> Self {
        ArmadaClient {
            event_client: EventClient::new(channel.clone()),
            submit_client: SubmitClient::new(channel.clone()),
        }
    }
    pub async fn get_job_events_stream(
        &mut self,
        queue: String,
        id: String,
        from_message_id: String,
    ) -> Result<tonic::Response<tonic::Streaming<EventStreamMessage>>, tonic::Status> {
        let jsr = JobSetRequest {
            queue,
            id,
            from_message_id,
            watch: true,
            error_if_missing: true,
            // these fields are only for testing
            force_legacy: false,
            force_new: false,
        };
        self.event_client.get_job_set_events(jsr).await
    }

    pub async fn submit_health(
        &mut self,
    ) -> Result<tonic::Response<HealthCheckResponse>, tonic::Status> {
        self.submit_client.health(()).await
    }

    pub async fn event_health(
        &mut self,
    ) -> Result<tonic::Response<HealthCheckResponse>, tonic::Status> {
        self.event_client.health(()).await
    }

    pub async fn submit_jobs(
        &mut self,
        queue: String,
        job_set_id: String,
        job_request_items: Vec<JobSubmitRequestItem>,
    ) -> Result<tonic::Response<JobSubmitResponse>, tonic::Status> {
        let request = JobSubmitRequest {
            queue,
            job_set_id,
            job_request_items,
        };
        self.submit_client.submit_jobs(request).await
    }

    pub async fn cancel_jobs(
        &mut self,
        queue: String,
        job_id: String,
        job_set_id: String,
    ) -> Result<tonic::Response<CancellationResult>, tonic::Status> {
        let request = JobCancelRequest {
            queue,
            job_id,
            job_set_id,
        };
        self.submit_client.cancel_jobs(request).await
    }

    pub async fn cancel_jobset(
        &mut self,
        queue: String,
        job_set_id: String,
        filter_states: Vec<JobState>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        let filter = JobSetFilter {
            states: filter_states
                .into_iter()
                .map(|s| s.try_into().unwrap())
                .collect(),
        };
        let request = JobSetCancelRequest {
            queue,
            job_set_id,
            filter: Some(filter),
        };
        self.submit_client.cancel_job_set(request).await
    }

    pub async fn reprioritize_jobs(
        &mut self,
        new_priority: f64,
        job_ids: Vec<String>,
        job_set_id: String,
        queue: String,
    ) -> Result<tonic::Response<JobReprioritizeResponse>, tonic::Status> {
        let request = JobReprioritizeRequest {
            job_ids,
            job_set_id,
            queue,
            new_priority,
        };
        self.submit_client.reprioritize_jobs(request).await
    }

    pub async fn create_queue(
        &mut self,
        queue: impl tonic::IntoRequest<Queue>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        self.submit_client.create_queue(queue).await
    }

    pub async fn update_queue(
        &mut self,
        queue: impl tonic::IntoRequest<Queue>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        self.submit_client.update_queue(queue).await
    }

    pub async fn create_queues(
        &mut self,
        queues: Vec<Queue>,
    ) -> Result<tonic::Response<BatchQueueCreateResponse>, tonic::Status> {
        let queue_list = QueueList { queues };
        self.submit_client.create_queues(queue_list).await
    }

    pub async fn update_queues(
        &mut self,
        queues: Vec<Queue>,
    ) -> Result<tonic::Response<BatchQueueUpdateResponse>, tonic::Status> {
        let queue_list = QueueList { queues };
        self.submit_client.update_queues(queue_list).await
    }

    pub async fn delete_queue(
        &mut self,
        name: String,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        self.submit_client
            .delete_queue(QueueDeleteRequest { name })
            .await
    }

    pub async fn get_queue(
        &mut self,
        name: String,
    ) -> Result<tonic::Response<Queue>, tonic::Status> {
        self.submit_client.get_queue(QueueGetRequest { name }).await
    }

    pub async fn get_queue_info(
        &mut self,
        name: String,
    ) -> Result<tonic::Response<QueueInfo>, tonic::Status> {
        self.submit_client
            .get_queue_info(QueueInfoRequest { name })
            .await
    }
}
