use armada_client::{BasicAuthProvider, StaticTokenProvider, TokenProvider};

/// Build a [`TokenProvider`] from environment variables.
///
/// Priority:
/// 1. `ARMADA_USER` + `ARMADA_PASS` → Basic Auth
/// 2. `ARMADA_TOKEN`               → Bearer token
/// 3. Neither set                  → no auth (warns; suitable for anonymous clusters)
pub fn auth_from_env() -> Box<dyn TokenProvider + Send + Sync> {
    if let (Ok(user), Ok(pass)) = (std::env::var("ARMADA_USER"), std::env::var("ARMADA_PASS")) {
        println!("Using Basic Auth as '{user}'");
        Box::new(BasicAuthProvider::new(user, pass))
    } else {
        let token = std::env::var("ARMADA_TOKEN").unwrap_or_default();
        if token.is_empty() {
            eprintln!(
                "Warning: no auth set (ARMADA_USER/ARMADA_PASS or ARMADA_TOKEN) — \
                 requests may be rejected by the server"
            );
        }
        Box::new(StaticTokenProvider::new(token))
    }
}
