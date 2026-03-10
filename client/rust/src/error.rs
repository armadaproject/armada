use thiserror::Error;

/// All errors that can be returned by the Armada client.
///
/// Most callers can pattern-match on the variant that matters to them and
/// propagate the rest with `?`. Use [`Error::auth`] as a convenience
/// constructor when implementing [`crate::auth::TokenProvider`].
///
/// # Example
///
/// ```no_run
/// use armada_client::{ArmadaClient, Error, StaticTokenProvider};
///
/// async fn submit_or_retry(client: &ArmadaClient) {
///     // ... build request ...
/// # let request = armada_client::JobSubmitRequest {
/// #     queue: "q".into(), job_set_id: "js".into(), job_request_items: vec![],
/// # };
///     match client.submit(request).await {
///         Ok(resp) => { /* handle success */ }
///         Err(Error::Grpc(status)) => {
///             // The server returned a non-OK gRPC status (e.g. NOT_FOUND).
///             eprintln!("gRPC error: {status}");
///         }
///         Err(Error::Auth(msg)) => {
///             // Token provider failed — check credentials.
///             eprintln!("auth failure: {msg}");
///         }
///         Err(e) => eprintln!("other error: {e}"),
///     }
/// }
/// ```
#[derive(Debug, Error)]
pub enum Error {
    /// A low-level transport failure occurred before a gRPC response could be
    /// received — for example a connection refused, TLS handshake error, or
    /// DNS resolution failure.
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),

    /// The server returned a non-OK gRPC status code.
    ///
    /// Common codes:
    /// - `NOT_FOUND` — the requested queue or job set does not exist.
    /// - `PERMISSION_DENIED` — the bearer token lacks the required privileges.
    /// - `INVALID_ARGUMENT` — the request payload was rejected by the server.
    #[error("gRPC error: {0}")]
    Grpc(#[source] Box<tonic::Status>),

    /// Returned by custom [`crate::auth::TokenProvider`] implementations
    /// to signal that token retrieval failed.
    #[error("auth error: {0}")]
    Auth(String),

    /// Returned when the endpoint string passed to
    /// [`crate::client::ArmadaClient::connect`] or
    /// [`crate::client::ArmadaClient::connect_tls`] is not a valid URI.
    #[error("invalid endpoint URI: {0}")]
    InvalidUri(String),

    /// The bearer token string contained characters that are not valid in an
    /// HTTP header value. Token strings must be ASCII and must not contain
    /// control characters (`\0`, `\r`, `\n`, etc.).
    #[error("invalid metadata value: {0}")]
    InvalidMetadata(#[from] tonic::metadata::errors::InvalidMetadataValue),
}

impl From<tonic::Status> for Error {
    fn from(s: tonic::Status) -> Self {
        Self::Grpc(Box::new(s))
    }
}

impl Error {
    /// Convenience constructor for [`Error::Auth`].
    ///
    /// Use this in custom [`crate::auth::TokenProvider`] implementations:
    /// ```ignore
    /// return Err(Error::auth("token expired"));
    /// ```
    pub fn auth(msg: impl Into<String>) -> Self {
        Self::Auth(msg.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_status() {
        let status = tonic::Status::not_found("not found");
        let err = Error::from(status);
        assert!(matches!(err, Error::Grpc(_)));
    }

    #[test]
    fn from_invalid_metadata() {
        let bad: Result<tonic::metadata::MetadataValue<tonic::metadata::Ascii>, _> = "\x00".parse();
        let err = Error::from(bad.unwrap_err());
        assert!(matches!(err, Error::InvalidMetadata(_)));
    }

    #[test]
    fn invalid_uri_holds_message() {
        let err = Error::InvalidUri("not a uri".to_string());
        assert!(err.to_string().contains("not a uri"));
    }

    #[test]
    fn auth_constructor() {
        let err = Error::auth("token expired");
        assert!(matches!(err, Error::Auth(_)));
        assert_eq!(err.to_string(), "auth error: token expired");
    }
}
