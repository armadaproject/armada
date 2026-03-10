use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use futures::stream::BoxStream;
use tonic::transport::{Channel, ClientTlsConfig};
use tracing::instrument;

use crate::api::{
    EventStreamMessage, JobSetRequest, JobSubmitRequest, JobSubmitResponse,
    event_client::EventClient, submit_client::SubmitClient,
};
use crate::auth::TokenProvider;
use crate::error::Error;

/// Armada gRPC client providing job submission and event-stream watching.
///
/// # Construction
///
/// Use [`ArmadaClient::connect`] for plaintext (dev / in-cluster) connections
/// and [`ArmadaClient::connect_tls`] for production clusters behind TLS.
/// Both accept any [`TokenProvider`] — pass [`crate::StaticTokenProvider`] for
/// a static bearer token or supply your own implementation for dynamic auth.
///
/// ```no_run
/// # use armada_client::{ArmadaClient, StaticTokenProvider};
/// # async fn example() -> Result<(), armada_client::Error> {
/// // Plaintext
/// let client = ArmadaClient::connect("http://localhost:50051", StaticTokenProvider::new("tok"))
///     .await?;
///
/// // TLS (uses system root certificates)
/// let client = ArmadaClient::connect_tls("https://armada.example.com:443", StaticTokenProvider::new("tok"))
///     .await?;
/// # Ok(())
/// # }
/// ```
///
/// # Cloning
///
/// `ArmadaClient` is `Clone`. All clones share the same underlying channel and
/// connection pool — cloning is `O(1)` and is the correct way to distribute the
/// client across tasks:
///
/// ```no_run
/// # use armada_client::{ArmadaClient, StaticTokenProvider};
/// # async fn example() -> Result<(), armada_client::Error> {
/// let client = ArmadaClient::connect("http://localhost:50051", StaticTokenProvider::new("tok"))
///     .await?;
///
/// let c1 = client.clone();
/// let c2 = client.clone();
/// tokio::spawn(async move { /* use c1 */ });
/// tokio::spawn(async move { /* use c2 */ });
/// # Ok(())
/// # }
/// ```
///
/// # Timeouts
///
/// Apply a per-call deadline with [`ArmadaClient::with_timeout`]. When set,
/// every RPC is governed by the deadline for its **entire duration**. For
/// unary calls (`submit`) the deadline covers the round-trip. For streaming
/// calls (`watch`) it covers the full lifetime of the stream — if the timeout
/// elapses while events are still being received the stream is cancelled with
/// a `DEADLINE_EXCEEDED` status.
#[derive(Clone)]
pub struct ArmadaClient {
    submit_client: SubmitClient<Channel>,
    event_client: EventClient<Channel>,
    token_provider: Arc<dyn TokenProvider + Send + Sync>,
    timeout: Option<Duration>,
}

impl ArmadaClient {
    /// Connect to an Armada server at `endpoint` using plaintext (no TLS).
    ///
    /// `endpoint` must be a valid URI, e.g. `"http://localhost:50051"`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidUri`] if the URI is malformed, or
    /// [`Error::Transport`] if the connection cannot be established.
    pub async fn connect(
        endpoint: impl Into<String>,
        token_provider: impl TokenProvider + 'static,
    ) -> Result<Self, Error> {
        let channel = Channel::from_shared(endpoint.into())
            .map_err(|e| Error::InvalidUri(e.to_string()))?
            .connect()
            .await?;
        Ok(Self::from_parts(channel, token_provider))
    }

    /// Connect to an Armada server at `endpoint` using TLS.
    ///
    /// Uses the system's native root certificates to verify the server
    /// certificate. `endpoint` should use the `https://` scheme,
    /// e.g. `"https://armada.example.com:443"`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidUri`] if the URI is malformed, or
    /// [`Error::Transport`] if TLS configuration or the connection fails.
    pub async fn connect_tls(
        endpoint: impl Into<String>,
        token_provider: impl TokenProvider + 'static,
    ) -> Result<Self, Error> {
        let channel = Channel::from_shared(endpoint.into())
            .map_err(|e| Error::InvalidUri(e.to_string()))?
            .tls_config(ClientTlsConfig::new())?
            .connect()
            .await?;
        Ok(Self::from_parts(channel, token_provider))
    }

    fn from_parts(channel: Channel, token_provider: impl TokenProvider + 'static) -> Self {
        Self {
            submit_client: SubmitClient::new(channel.clone()),
            event_client: EventClient::new(channel),
            token_provider: Arc::new(token_provider),
            timeout: None,
        }
    }

    /// Set a default timeout applied to every RPC call.
    ///
    /// When the timeout elapses before the server responds, the call fails with
    /// [`Error::Grpc`] wrapping a `DEADLINE_EXCEEDED` status. Note that for
    /// streaming calls like [`ArmadaClient::watch`], the deadline is applied to
    /// the initial connection, not to individual messages on the stream.
    ///
    /// Returns `self` so the call can be chained directly after construction:
    ///
    /// ```no_run
    /// # use std::time::Duration;
    /// # use armada_client::{ArmadaClient, StaticTokenProvider};
    /// # async fn example() -> Result<(), armada_client::Error> {
    /// let client = ArmadaClient::connect("http://localhost:50051", StaticTokenProvider::new("tok"))
    ///     .await?
    ///     .with_timeout(Duration::from_secs(30));
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    fn apply_timeout<T>(&self, req: &mut tonic::Request<T>) {
        if let Some(t) = self.timeout {
            req.set_timeout(t);
        }
    }

    /// Submit a batch of jobs to Armada.
    ///
    /// Attaches an `authorization` header on every call using the configured
    /// [`TokenProvider`] (e.g. `Bearer <token>` or `Basic <credentials>`). Multiple job items can be included in
    /// a single request — they are all submitted atomically to the same queue
    /// and job set.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use armada_client::{
    ///     ArmadaClient, JobRequestItemBuilder, JobSubmitRequest, StaticTokenProvider,
    /// };
    /// use armada_client::k8s::io::api::core::v1::PodSpec;
    ///
    /// # async fn example() -> Result<(), armada_client::Error> {
    /// # let client = ArmadaClient::connect("http://localhost:50051", StaticTokenProvider::new("")).await?;
    /// let item = JobRequestItemBuilder::new()
    ///     .namespace("default")
    ///     .pod_spec(PodSpec { containers: vec![], ..Default::default() })
    ///     .build();
    ///
    /// let response = client
    ///     .submit(JobSubmitRequest {
    ///         queue: "my-queue".into(),
    ///         job_set_id: "my-job-set".into(),
    ///         job_request_items: vec![item],
    ///     })
    ///     .await?;
    ///
    /// for r in &response.job_response_items {
    ///     if r.error.is_empty() {
    ///         println!("submitted: {}", r.job_id);
    ///     } else {
    ///         eprintln!("rejected: {}", r.error);
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// - [`Error::Auth`] if the token provider fails.
    /// - [`Error::InvalidMetadata`] if the token contains invalid header characters.
    /// - [`Error::Grpc`] if the server returns a non-OK status.
    #[instrument(skip(self, request), fields(queue = %request.queue, job_set_id = %request.job_set_id))]
    pub async fn submit(&self, request: JobSubmitRequest) -> Result<JobSubmitResponse, Error> {
        let token = self.token_provider.token().await?;
        let mut req = tonic::Request::new(request);
        if !token.is_empty() {
            req.metadata_mut().insert("authorization", token.parse()?);
        }
        self.apply_timeout(&mut req);
        let resp = self.submit_client.clone().submit_jobs(req).await?;
        Ok(resp.into_inner())
    }

    /// Watch a job set, returning a stream of events.
    ///
    /// Opens a server-streaming gRPC call and returns a [`BoxStream`] that
    /// yields [`EventStreamMessage`] values as the server pushes them. The
    /// stream ends when the server closes the connection.
    ///
    /// **Reconnection is the caller's responsibility.** Store the last
    /// `message_id` you received and pass it back as `from_message_id` when
    /// reconnecting to avoid replaying events you have already processed.
    ///
    /// # Arguments
    ///
    /// * `queue` — Armada queue name.
    /// * `job_set_id` — Job set to watch.
    /// * `from_message_id` — Optional resume cursor. Pass `Some(id)` to
    ///   receive only events that occurred after `id`; pass `None` to receive
    ///   all events from the beginning.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use futures::StreamExt;
    /// use armada_client::{ArmadaClient, StaticTokenProvider};
    ///
    /// # async fn example() -> Result<(), armada_client::Error> {
    /// # let client = ArmadaClient::connect("http://localhost:50051", StaticTokenProvider::new("")).await?;
    /// let mut stream = client
    ///     .watch("my-queue", "my-job-set", None)
    ///     .await?;
    ///
    /// let mut last_id = String::new();
    /// while let Some(result) = stream.next().await {
    ///     match result {
    ///         Ok(msg) => {
    ///             last_id = msg.id.clone();
    ///             println!("event id={} message={:?}", msg.id, msg.message);
    ///         }
    ///         Err(e) => {
    ///             eprintln!("stream error: {e}");
    ///             break; // reconnect using `from_message_id: Some(last_id)`
    ///         }
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// - [`Error::Auth`] if the token provider fails.
    /// - [`Error::InvalidMetadata`] if the token contains invalid header characters.
    /// - [`Error::Grpc`] immediately if the job set does not exist on the server,
    ///   or if the server returns a non-OK status on the initial call.
    /// - Individual stream items may also be [`Err(Error::Grpc)`] if the server
    ///   sends a trailing error status.
    #[instrument(skip_all, fields(queue, job_set_id))]
    pub async fn watch(
        &self,
        queue: impl Into<String>,
        job_set_id: impl Into<String>,
        from_message_id: Option<String>,
    ) -> Result<BoxStream<'static, Result<EventStreamMessage, Error>>, Error> {
        let queue: String = queue.into();
        let job_set_id: String = job_set_id.into();
        tracing::Span::current()
            .record("queue", queue.as_str())
            .record("job_set_id", job_set_id.as_str());

        let token = self.token_provider.token().await?;
        let job_set_request = JobSetRequest {
            id: job_set_id,
            queue,
            from_message_id: from_message_id.unwrap_or_default(),
            watch: true,
            error_if_missing: true,
        };
        let mut req = tonic::Request::new(job_set_request);
        if !token.is_empty() {
            req.metadata_mut().insert("authorization", token.parse()?);
        }
        self.apply_timeout(&mut req);
        let stream = self
            .event_client
            .clone()
            .get_job_set_events(req)
            .await?
            .into_inner();
        Ok(Box::pin(stream.map(|r| r.map_err(Error::Grpc))))
    }
}
