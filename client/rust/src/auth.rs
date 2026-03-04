use futures::future::BoxFuture;

use crate::error::Error;

/// Provides an `Authorization` header value for each outgoing gRPC call.
///
/// Implement this trait to integrate with any auth source — static bearer
/// token, HTTP Basic Auth, OAuth2, OIDC, service-account token rotation, etc.
///
/// # Contract
///
/// [`token`](TokenProvider::token) must return the **complete** value to be
/// set in the `authorization` gRPC metadata field, including the scheme
/// prefix:
///
/// - Bearer auth: `"Bearer <token>"`
/// - Basic auth: `"Basic <base64(user:pass)>"`
/// - No auth (unauthenticated clusters): `""` — the client omits the header.
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
///             Ok("Bearer my-dynamic-token".to_string())
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
    /// Retrieve the current `Authorization` header value asynchronously.
    ///
    /// Return the full scheme-prefixed value (e.g. `"Bearer <token>"`) or an
    /// empty string to send no `Authorization` header. The client calls this
    /// before every RPC; implementations that cache tokens should handle expiry
    /// and refresh internally.
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
        let token = token.into();
        Self {
            token: if token.is_empty() {
                token
            } else {
                format!("Bearer {token}")
            },
        }
    }
}

impl TokenProvider for Box<dyn TokenProvider + Send + Sync> {
    fn token(&self) -> BoxFuture<'_, Result<String, Error>> {
        (**self).token()
    }
}

impl TokenProvider for StaticTokenProvider {
    fn token(&self) -> BoxFuture<'_, Result<String, Error>> {
        let token = self.token.clone();
        Box::pin(async move { Ok(token) })
    }
}

/// A [`TokenProvider`] that authenticates using HTTP Basic Auth.
///
/// Encodes `username:password` as Base64 and returns the full
/// `Basic <credentials>` header value. Suitable for Armada clusters
/// configured with `basicAuth.enableAuthentication: true`.
///
/// # Example
///
/// ```
/// use armada_client::BasicAuthProvider;
///
/// let provider = BasicAuthProvider::new("admin", "admin");
/// ```
pub struct BasicAuthProvider {
    header: String,
}

impl BasicAuthProvider {
    /// Create a new `BasicAuthProvider` from a username and password.
    pub fn new(username: impl AsRef<str>, password: impl AsRef<str>) -> Self {
        use base64::Engine as _;
        let raw = format!("{}:{}", username.as_ref(), password.as_ref());
        let encoded = base64::engine::general_purpose::STANDARD.encode(raw);
        Self {
            header: format!("Basic {encoded}"),
        }
    }
}

impl std::fmt::Debug for BasicAuthProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BasicAuthProvider")
            .field("header", &"[redacted]")
            .finish()
    }
}

impl TokenProvider for BasicAuthProvider {
    fn token(&self) -> BoxFuture<'_, Result<String, Error>> {
        let header = self.header.clone();
        Box::pin(async move { Ok(header) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn static_provider_returns_bearer_header() {
        let provider = StaticTokenProvider::new("tok");
        assert_eq!(provider.token().await.unwrap(), "Bearer tok");
    }

    #[tokio::test]
    async fn static_provider_empty_token_returns_empty() {
        let provider = StaticTokenProvider::new("");
        assert_eq!(provider.token().await.unwrap(), "");
    }

    #[tokio::test]
    async fn basic_provider_returns_basic_header() {
        let provider = BasicAuthProvider::new("admin", "admin");
        let result = provider.token().await.unwrap();
        // base64("admin:admin") = "YWRtaW46YWRtaW4="
        assert_eq!(result, "Basic YWRtaW46YWRtaW4=");
    }
}
