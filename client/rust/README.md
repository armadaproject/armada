# Armada Rust Client

Rust gRPC client for the [Armada](https://armadaproject.io) batch job scheduler.

Provides two operations:
- **`submit`** — submit a batch of jobs to a queue
- **`watch`** — stream events for a job set

---

## Prerequisites

| Tool | Purpose | Install |
|------|---------|---------|
| Rust ≥ 1.85 | Build toolchain | `curl https://sh.rustup.rs \| sh` |
| `protoc` | Proto code generation at build time | `brew install protobuf` (macOS) / `apt install protobuf-compiler` (Linux) |

Verify both are available:

```bash
rustc --version   # rustc 1.85.0 or newer
protoc --version  # libprotoc 23.x or newer
```

---

## Building

The crate lives inside the Armada monorepo. It does **not** participate in a Cargo workspace — all commands use `--manifest-path`.

```bash
# from the repo root
cargo build --manifest-path client/rust/Cargo.toml
```

What happens during `cargo build`:

1. `build.rs` runs `tonic-build` in two passes:
   - **Pass 1** — compiles vendored k8s protos (`client/rust/proto/k8s.io/…`) and generates their Rust types into `$OUT_DIR`.
   - **Pass 2** — compiles the Armada API protos (`pkg/api/*.proto`) with `extern_path` entries that redirect k8s type references to the types generated in Pass 1, avoiding duplicate definitions.
2. The generated `.rs` files are included into the crate via `tonic::include_proto!` in `src/lib.rs`.

---

## Running Tests

```bash
cargo test --manifest-path client/rust/Cargo.toml
```

Expected output:

```
running 12 tests
test auth::tests::basic_provider_returns_basic_header ... ok
test auth::tests::static_provider_empty_token_returns_empty ... ok
test auth::tests::static_provider_returns_bearer_header ... ok
test builder::tests::builder_with_pod_specs_builds_correctly ... ok
test builder::tests::label_and_annotation_helpers ... ok
test builder::tests::optional_fields_default_to_empty ... ok
test builder::tests::pod_spec_called_twice_replaces_previous ... ok
test builder::tests::pod_spec_singular_shorthand ... ok
test error::tests::auth_constructor ... ok
test error::tests::from_invalid_metadata ... ok
test error::tests::from_status ... ok
test error::tests::invalid_uri_holds_message ... ok

test result: ok. 12 passed; 0 failed
```

Doc-tests are disabled (`doctest = false`) because the vendored k8s and google API proto files contain Go/YAML syntax in their doc comments that is not valid Rust.

---

## Running the Examples

Two runnable examples live in `examples/`. Both read configuration from environment variables.

| Variable | Default | Description |
|----------|---------|-------------|
| `ARMADA_ENDPOINT` | `http://localhost:50051` | gRPC endpoint |
| `ARMADA_TOKEN` | _(empty)_ | Bearer token; leave empty for unauthenticated clusters |
| `ARMADA_QUEUE` | `test` | Queue name (must exist) |
| `ARMADA_JOB_SET` | `rust-smoke-test` | Job set ID (arbitrary string) |

### 1. Create a queue

Use the Armada REST API (port 8080 / NodePort 30001 in kind):

```bash
curl -s -X POST http://localhost:30001/v1/queue \
  -H 'Content-Type: application/json' \
  -d '{"name":"rust-test","priorityFactor":1}'
```

### 2. Submit jobs

Submits one job and prints the assigned job ID, then prints the command to watch it:

```bash
ARMADA_ENDPOINT=http://localhost:30002 \
ARMADA_QUEUE=rust-test \
ARMADA_JOB_SET=rust-smoke-$(date +%s) \
cargo run --manifest-path client/rust/Cargo.toml --example submit
```

Expected output:

```
Submitting job to queue 'rust-test', job set 'rust-smoke-1234567890'...
Submitted 1 job(s):
  job_id=01kjsxm5ksstr57prfqjtgakq0

To watch this job set:
  ARMADA_ENDPOINT=http://localhost:30002 ARMADA_QUEUE=rust-test ARMADA_JOB_SET=rust-smoke-1234567890 \
  cargo run --manifest-path client/rust/Cargo.toml --example watch
```

### 3. Watch a job set

Streams events for a job set until the server closes the connection (Ctrl-C to exit):

```bash
ARMADA_ENDPOINT=http://localhost:30002 \
ARMADA_QUEUE=rust-test \
ARMADA_JOB_SET=rust-smoke-1234567890 \
cargo run --manifest-path client/rust/Cargo.toml --example watch
```

Expected output:

```
Watching job set 'rust-smoke-1234567890' on queue 'rust-test'...
  event id=…  message=Some(EventMessage { events: Some(Submitted(…)) })
  event id=…  message=Some(EventMessage { events: Some(Queued(…)) })
  event id=…  message=Some(EventMessage { events: Some(Leased(…)) })
  event id=…  message=Some(EventMessage { events: Some(Pending(…)) })
  event id=…  message=Some(EventMessage { events: Some(Running(…)) })
  event id=…  message=Some(EventMessage { events: Some(Succeeded(…)) })
```

---

## Using the Client in Your Code

Add the crate as a path dependency (until it is published to crates.io):

```toml
[dependencies]
armada-client = { path = "../armada/client/rust" }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
futures = "0.3"
```

### Connect

```rust
use armada_client::{ArmadaClient, StaticTokenProvider};

// Plaintext (development / in-cluster without TLS)
let client = ArmadaClient::connect("http://localhost:50051", StaticTokenProvider::new("my-token")).await?;

// TLS (production — uses system root certificates)
let client = ArmadaClient::connect_tls("https://armada.example.com:443", StaticTokenProvider::new("my-token")).await?;

// Optional: apply a deadline to every RPC
use std::time::Duration;
let client = client.with_timeout(Duration::from_secs(30));
```

`ArmadaClient` is `Clone` — all clones share the same underlying channel, so cloning is the right way to distribute the client across tasks.

### Submit jobs

```rust
use armada_client::{JobRequestItemBuilder, JobSubmitRequest};
use armada_client::k8s::io::api::core::v1::{Container, PodSpec, ResourceRequirements};
use armada_client::k8s::io::apimachinery::pkg::api::resource::Quantity;

// k8s protos use proto2 syntax — optional fields are Option<T>
let cpu = Quantity { string: Some("100m".to_string()), ..Default::default() };
let memory = Quantity { string: Some("64Mi".to_string()), ..Default::default() };

let container = Container {
    name: Some("main".to_string()),
    image: Some("busybox:latest".to_string()),
    command: vec!["sh".to_string()],
    args: vec!["-c".to_string(), "echo hello".to_string()],
    resources: Some(ResourceRequirements {
        requests: [("cpu".to_string(), cpu.clone()), ("memory".to_string(), memory.clone())]
            .into_iter()
            .collect(),
        limits: [("cpu".to_string(), cpu), ("memory".to_string(), memory)]
            .into_iter()
            .collect(),
        ..Default::default()
    }),
    ..Default::default()
};

let pod_spec = PodSpec {
    containers: vec![container],
    ..Default::default()
};

// Typestate builder: .build() only compiles after .pod_spec() / .pod_specs() is called
let item = JobRequestItemBuilder::new()
    .namespace("default")
    .priority(1.0)
    .label("app", "my-app")
    .pod_spec(pod_spec)
    .build();

let response = client.submit(JobSubmitRequest {
    queue: "my-queue".to_string(),
    job_set_id: "my-job-set".to_string(),
    job_request_items: vec![item],
}).await?;

for r in &response.job_response_items {
    if r.error.is_empty() {
        println!("submitted job_id={}", r.job_id);
    } else {
        eprintln!("error: {}", r.error);
    }
}
```

### Watch a job set

```rust
use futures::StreamExt;

let mut stream = client.watch("my-queue", "my-job-set", None).await?;
while let Some(event) = stream.next().await {
    match event {
        Ok(msg) => println!("event id={} msg={:?}", msg.id, msg.message),
        Err(e) => { eprintln!("stream error: {e}"); break; }
    }
}
```

Pass a `from_message_id` to resume from a known cursor after reconnecting:

```rust
let mut stream = client.watch("my-queue", "my-job-set", Some(last_id)).await?;
```

---

## API Reference

### `ArmadaClient`

| Method | Signature | Description |
|--------|-----------|-------------|
| `connect` | `(endpoint, provider) -> Result<Self>` | Open a plaintext gRPC channel |
| `connect_tls` | `(endpoint, provider) -> Result<Self>` | Open a TLS channel (system roots) |
| `with_timeout` | `(Duration) -> Self` | Set a per-call deadline (chainable) |
| `submit` | `(JobSubmitRequest) -> Result<JobSubmitResponse>` | Submit one or more jobs |
| `watch` | `(queue, job_set_id, from_message_id) -> Result<BoxStream<…>>` | Stream job set events |

### `JobRequestItemBuilder<S>`

Typestate builder. All setters are available in any state. `.build()` requires the `HasPodSpec` state, which is entered by calling `.pod_spec(spec)` or `.pod_specs(specs)`.

| Setter | Type | Default |
|--------|------|---------|
| `.namespace(s)` | `impl Into<String>` | `""` |
| `.priority(p)` | `f64` | `0.0` |
| `.client_id(s)` | `impl Into<String>` | `""` |
| `.label(k, v)` | `impl Into<String>` × 2 | — adds one label |
| `.labels(m)` | `HashMap<String, String>` | empty |
| `.annotation(k, v)` | `impl Into<String>` × 2 | — adds one annotation |
| `.annotations(m)` | `HashMap<String, String>` | empty |
| `.scheduler(s)` | `impl Into<String>` | `""` |
| `.add_ingress(i)` | `IngressConfig` | — appends one config |
| `.ingress(v)` | `Vec<IngressConfig>` | empty |
| `.add_service(s)` | `ServiceConfig` | — appends one config |
| `.services(v)` | `Vec<ServiceConfig>` | empty |
| `.pod_spec(s)` | `PodSpec` | — transitions to `HasPodSpec` |
| `.pod_specs(v)` | `Vec<PodSpec>` | — transitions to `HasPodSpec` |

### `StaticTokenProvider`

Implements `TokenProvider` with a fixed token string. Its `Debug` output redacts the token value.

```rust
let p = StaticTokenProvider::new("secret");
println!("{p:?}");  // StaticTokenProvider { token: "[redacted]" }
```

### `Error`

| Variant | Cause |
|---------|-------|
| `Transport(tonic::transport::Error)` | TCP/TLS connection failure |
| `Grpc(tonic::Status)` | Server returned a non-OK gRPC status |
| `Auth(String)` | Token provider returned an error |
| `InvalidUri(String)` | Malformed endpoint string |
| `InvalidMetadata(…)` | Token contains characters invalid in HTTP headers |

---

## Project Structure

```
client/rust/
├── build.rs                   # tonic-build: two-pass proto compilation
├── Cargo.toml
├── proto/                     # Vendored protos (k8s + google.api)
│   ├── google/api/            # google.api HTTP annotations
│   └── k8s.io/                # k8s API + apimachinery types
├── src/
│   ├── lib.rs                 # Module layout + public re-exports
│   ├── auth.rs                # TokenProvider trait, StaticTokenProvider
│   ├── builder.rs             # JobRequestItemBuilder (typestate)
│   ├── client.rs              # ArmadaClient
│   └── error.rs               # Error enum
└── examples/
    ├── submit.rs              # Submit a job and print the job ID
    └── watch.rs               # Stream events for a job set
```

---

## CI

GitHub Actions workflow: `.github/workflows/rust-client.yml`

Runs on every push/PR that touches `client/rust/**`:

1. `cargo fmt -- --check`
2. `cargo build`
3. `cargo test`
4. `cargo clippy -- -D warnings`

`protoc` is installed in CI via `arduino/setup-protoc@v3` before the build step.
