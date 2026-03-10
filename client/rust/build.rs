fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Without these directives Cargo's default is to re-run build.rs on every
    // source change. Restrict re-runs to changes that actually affect codegen.
    println!("cargo:rerun-if-changed=proto/");
    println!("cargo:rerun-if-changed=build.rs");
    // Only emit monorepo paths when they exist; cargo treats missing
    // rerun-if-changed paths as "always rerun", which would hurt consumers
    // of the published crate where ../../pkg/api/ is not present.
    if std::path::Path::new("../../pkg/api/submit.proto").exists() {
        println!("cargo:rerun-if-changed=../../pkg/api/submit.proto");
        println!("cargo:rerun-if-changed=../../pkg/api/event.proto");
        println!("cargo:rerun-if-changed=../../pkg/api/job.proto");
        println!("cargo:rerun-if-changed=../../pkg/api/health.proto");
    }

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
    //
    // When building inside the monorepo the protos live at ../../pkg/api/ relative
    // to this manifest. When building from a crates.io package they are vendored
    // into proto/pkg/api/ (mirroring the import paths used inside the .proto files).
    // NOTE: This probe assumes the crate lives exactly two directories below the
    // monorepo root (i.e. <root>/client/rust/). If the crate is relocated,
    // update these relative paths accordingly.
    let (armada_protos, armada_includes): (Vec<&str>, Vec<&str>) =
        if std::path::Path::new("../../pkg/api/submit.proto").exists() {
            (
                vec![
                    "../../pkg/api/submit.proto",
                    "../../pkg/api/event.proto",
                    "../../pkg/api/job.proto",
                    "../../pkg/api/health.proto",
                ],
                vec!["../../", "proto"],
            )
        } else {
            // Vendored layout used by the published crate.
            (
                vec![
                    "proto/pkg/api/submit.proto",
                    "proto/pkg/api/event.proto",
                    "proto/pkg/api/job.proto",
                    "proto/pkg/api/health.proto",
                ],
                vec!["proto"],
            )
        };

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
        .compile_protos(&armada_protos, &armada_includes)?;

    Ok(())
}
