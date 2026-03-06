fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Pass 1: compile vendored k8s protos to generate their Rust types.
    // No extern_path — we want these files emitted into OUT_DIR.
    tonic_build::configure()
        .build_client(false)
        .build_server(false)
        .compile_protos(
            &[
                "proto/k8s.io/api/core/v1/generated.proto",
                "proto/k8s.io/api/networking/v1/generated.proto",
                "proto/k8s.io/apimachinery/pkg/api/resource/generated.proto",
                "proto/k8s.io/apimachinery/pkg/apis/meta/v1/generated.proto",
                "proto/k8s.io/apimachinery/pkg/runtime/generated.proto",
                "proto/k8s.io/apimachinery/pkg/util/intstr/generated.proto",
            ],
            &["proto"],
        )?;

    // Pass 2: compile Armada API protos.
    // extern_path tells prost that k8s types live in our module tree (from pass 1)
    // rather than generating duplicate inline copies inside api.rs.
    tonic_build::configure()
        .build_server(false)
        .extern_path(".k8s.io.api.core.v1", "crate::k8s::io::api::core::v1")
        .extern_path(
            ".k8s.io.api.networking.v1",
            "crate::k8s::io::api::networking::v1",
        )
        .extern_path(
            ".k8s.io.apimachinery.pkg.api.resource",
            "crate::k8s::io::apimachinery::pkg::api::resource",
        )
        .extern_path(
            ".k8s.io.apimachinery.pkg.apis.meta.v1",
            "crate::k8s::io::apimachinery::pkg::apis::meta::v1",
        )
        .extern_path(
            ".k8s.io.apimachinery.pkg.runtime",
            "crate::k8s::io::apimachinery::pkg::runtime",
        )
        .extern_path(
            ".k8s.io.apimachinery.pkg.util.intstr",
            "crate::k8s::io::apimachinery::pkg::util::intstr",
        )
        .compile_protos(
            &[
                "../../pkg/api/submit.proto",
                "../../pkg/api/event.proto",
                "../../pkg/api/job.proto",
                "../../pkg/api/health.proto",
            ],
            &["../../", "proto"],
        )?;

    Ok(())
}
