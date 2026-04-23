//! Rust client for the [Armada](https://armadaproject.io) batch-job scheduler.
//!
//! This crate provides an async gRPC client for submitting and monitoring batch
//! jobs on an Armada cluster. It is built on top of [tonic] and exposes a
//! small, ergonomic API that can be shared across async tasks without locking.
//!
//! # Quick start
//!
//! ```no_run
//! use armada_client::k8s::io::api::core::v1::{Container, PodSpec};
//! use armada_client::{
//!     ArmadaClient, JobRequestItemBuilder, JobSubmitRequest, StaticTokenProvider,
//! };
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // 1. Connect (plaintext). Use `connect_tls` for production clusters.
//!     let client = ArmadaClient::connect(
//!         "http://localhost:50051",
//!         StaticTokenProvider::new("my-token"),
//!     )
//!     .await?;
//!
//!     // 2. Build a job item — pod spec required before `.build()` compiles.
//!     let pod_spec = PodSpec {
//!         containers: vec![Container {
//!             name: Some("main".into()),
//!             image: Some("busybox:latest".into()),
//!             command: vec!["echo".into()],
//!             args: vec!["hello".into()],
//!             ..Default::default()
//!         }],
//!         ..Default::default()
//!     };
//!
//!     let item = JobRequestItemBuilder::new()
//!         .namespace("default")
//!         .priority(1.0)
//!         .pod_spec(pod_spec)
//!         .build();
//!
//!     // 3. Submit.
//!     let response = client
//!         .submit(JobSubmitRequest {
//!             queue: "my-queue".into(),
//!             job_set_id: "my-job-set".into(),
//!             job_request_items: vec![item],
//!         })
//!         .await?;
//!
//!     for r in &response.job_response_items {
//!         println!("job_id={}", r.job_id);
//!     }
//!     Ok(())
//! }
//! ```
//!
//! # Features
//!
//! | Feature | How to use |
//! |---|---|
//! | Plaintext connection | [`ArmadaClient::connect`] |
//! | TLS connection (system roots) | [`ArmadaClient::connect_tls`] |
//! | Per-call timeout | [`ArmadaClient::with_timeout`] |
//! | Job submission | [`ArmadaClient::submit`] |
//! | Cancel jobs by ID | [`ArmadaClient::cancel_jobs`] |
//! | Cancel a job set | [`ArmadaClient::cancel_job_set`] |
//! | Event streaming | [`ArmadaClient::watch`] |
//! | Static bearer token | [`StaticTokenProvider`] |
//! | Custom auth (OIDC, OAuth2…) | Implement [`TokenProvider`] |
//! | Compile-time safe job builder | [`JobRequestItemBuilder`] |
//!
//! # Kubernetes proto types
//!
//! Armada job specs embed Kubernetes `PodSpec` and related types. These are
//! generated from the upstream k8s protobufs and re-exported under the
//! [`k8s`] module, mirroring the proto package hierarchy:
//!
//! ```ignore
//! use armada_client::k8s::io::api::core::v1::{Container, PodSpec, ResourceRequirements};
//! use armada_client::k8s::io::apimachinery::pkg::api::resource::Quantity;
//! ```
//!
//! # Cloning and concurrent use
//!
//! [`ArmadaClient`] is `Clone`. All clones share the same underlying
//! connection pool — cloning is cheap and the correct way to use the client
//! across multiple tasks:
//!
//! ```no_run
//! # use armada_client::{ArmadaClient, StaticTokenProvider};
//! # async fn example() -> Result<(), armada_client::Error> {
//! let client = ArmadaClient::connect("http://localhost:50051", StaticTokenProvider::new("tok"))
//!     .await?;
//!
//! let c1 = client.clone();
//! let c2 = client.clone();
//! tokio::join!(
//!     async move { /* use c1 */ },
//!     async move { /* use c2 */ },
//! );
//! # Ok(())
//! # }
//! ```

pub mod api {
    #![allow(
        clippy::tabs_in_doc_comments,
        clippy::doc_lazy_continuation,
        clippy::doc_overindented_list_items,
        clippy::large_enum_variant
    )]
    tonic::include_proto!("api");
}

// google.api types (HttpRule etc.) generated as a side-effect of compiling the
// Armada API protos. Not part of the public API — used internally by api.rs.
pub(crate) mod google {
    pub mod api {
        #![allow(
            dead_code,
            clippy::tabs_in_doc_comments,
            clippy::doc_lazy_continuation,
            clippy::doc_overindented_list_items,
            clippy::large_enum_variant
        )]
        tonic::include_proto!("google.api");
    }
}

// k8s types referenced by the api package — must mirror the proto package hierarchy
// so that generated api.rs cross-package refs (e.g., super::k8s::io::api::core::v1::PodSpec) resolve.
pub mod k8s {
    #![allow(
        clippy::tabs_in_doc_comments,
        clippy::doc_lazy_continuation,
        clippy::doc_overindented_list_items,
        clippy::large_enum_variant
    )]
    pub mod io {
        pub mod api {
            pub mod core {
                pub mod v1 {
                    tonic::include_proto!("k8s.io.api.core.v1");
                }
            }
            pub mod networking {
                pub mod v1 {
                    tonic::include_proto!("k8s.io.api.networking.v1");
                }
            }
        }
        pub mod apimachinery {
            pub mod pkg {
                pub mod api {
                    pub mod resource {
                        tonic::include_proto!("k8s.io.apimachinery.pkg.api.resource");
                    }
                }
                pub mod apis {
                    pub mod meta {
                        pub mod v1 {
                            tonic::include_proto!("k8s.io.apimachinery.pkg.apis.meta.v1");
                        }
                    }
                }
                pub mod runtime {
                    tonic::include_proto!("k8s.io.apimachinery.pkg.runtime");
                }
                pub mod util {
                    pub mod intstr {
                        tonic::include_proto!("k8s.io.apimachinery.pkg.util.intstr");
                    }
                }
            }
        }
    }
}

pub mod auth;
pub mod builder;
pub mod client;
pub mod error;

// Convenience re-exports for the public API surface
pub use api::{
    CancellationResult, EventMessage, EventStreamMessage, JobCancelRequest, JobSetCancelRequest,
    JobSetFilter, JobState, JobSubmitRequest, JobSubmitRequestItem, JobSubmitResponse,
};
pub use auth::{BasicAuthProvider, StaticTokenProvider, TokenProvider};
pub use builder::JobRequestItemBuilder;
pub use client::ArmadaClient;
pub use error::Error;
pub use futures::stream::BoxStream;
