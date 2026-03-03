use futures::future::BoxFuture;

use crate::error::Error;

/// Provides a bearer token for each outgoing gRPC call.
///
/// Implement this trait to integrate with any token source — static string,
/// OAuth2, OIDC, service-account token rotation, etc. The returned `String`
/// is placed in the `authorization: Bearer <token>` header on every RPC.
///
/// # Custom implementation
///
/// The future must be `Send` because the client may call it from any async
/// task. Use `Box::pin(async move { … })` to construct the return value:
///
/// ```no_run
/// use futures::future::BoxFuture;
/// use armada_client::{Error, TokenProvider};
///
/// struct MyTokenProvider;
///
/// impl TokenProvider for MyTokenProvider {
///     fn token(&self) -> BoxFuture<'_, Result<String, Error>> {
///         Box::pin(async move {
///             // Fetch or refresh from your auth backend here.
///             Ok("my-dynamic-token".to_string())
///         })
///     }
/// }
/// ```
///
/// Return [`Error::auth`] to signal that token retrieval failed:
///
/// ```no_run
/// # use futures::future::BoxFuture;
/// # use armada_client::{Error, TokenProvider};
/// # struct Failing;
/// # impl TokenProvider for Failing {
/// #     fn token(&self) -> BoxFuture<'_, Result<String, Error>> {
///         Box::pin(async move {
///             Err(Error::auth("token expired"))
///         })
/// #     }
/// # }
/// ```
pub trait TokenProvider: Send + Sync {
    /// Retrieve the current bearer token asynchronously.
    ///
    /// The client calls this method before every RPC. Implementations that
    /// cache tokens should handle expiry and refresh internally.
    fn token(&self) -> BoxFuture<'_, Result<String, Error>>;
}

/// A [`TokenProvider`] that always returns the same static bearer token.
///
/// Suitable for development, testing, or clusters where a single long-lived
/// token is acceptable. For production workloads with token rotation, implement
/// [`TokenProvider`] directly.
///
/// # Debug output
///
/// `StaticTokenProvider` implements [`Debug`] but redacts the token value so
/// that secrets are not accidentally leaked into logs:
///
/// ```
/// use armada_client::StaticTokenProvider;
///
/// let p = StaticTokenProvider::new("super-secret");
/// assert_eq!(format!("{p:?}"), "StaticTokenProvider { token: \"[redacted]\" }");
/// ```
///
/// [`Debug`]: std::fmt::Debug
pub struct StaticTokenProvider {
    token: String,
}

impl std::fmt::Debug for StaticTokenProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StaticTokenProvider")
            .field("token", &"[redacted]")
            .finish()
    }
}

impl StaticTokenProvider {
    /// Create a new `StaticTokenProvider` from any string-like value.
    ///
    /// Pass an empty string for unauthenticated clusters:
    ///
    /// ```
    /// use armada_client::StaticTokenProvider;
    ///
    /// let provider = StaticTokenProvider::new("my-bearer-token");
    /// let empty    = StaticTokenProvider::new("");   // unauthenticated
    /// ```
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
        }
    }
}

impl TokenProvider for StaticTokenProvider {
    fn token(&self) -> BoxFuture<'_, Result<String, Error>> {
        let token = self.token.clone();
        Box::pin(async move { Ok(token) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn static_provider_returns_token() {
        let provider = StaticTokenProvider::new("tok");
        let result = provider.token().await;
        assert_eq!(result.unwrap(), "tok".to_string());
    }
}
