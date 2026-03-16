fn copy_rs_files(
    from: &std::path::Path,
    to: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_some_and(|e| e == "rs") {
            std::fs::copy(&path, to.join(entry.file_name()))?;
        }
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Without these directives Cargo's default is to re-run build.rs on every
    // source change. Restrict re-runs to changes that actually affect codegen.
    println!("cargo:rerun-if-changed=proto/");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=src/gen/");
    println!("cargo:rerun-if-env-changed=ARMADA_GENERATE");

    let out_dir = std::env::var("OUT_DIR")?;
    let out_path = std::path::Path::new(&out_dir);
    let gen_dir = std::path::Path::new("src/gen");

    // Pre-generated files exist (published crate from crates.io): copy them to
    // OUT_DIR so that tonic::include_proto! resolves correctly, then return.
    // This path requires no protoc and no proto sources.
    if gen_dir.join("api.rs").exists() {
        copy_rs_files(gen_dir, out_path)?;
        return Ok(());
    }

    // NOTE: This probe assumes the crate lives exactly two directories below the
    // monorepo root (i.e. <root>/client/rust/). If the crate is relocated,
    // update these relative paths accordingly.
    //
    // Cached once so both the rerun-if-changed directives and the compile_protos
    // call use the same decision, preventing a future refactor from accidentally
    // mismatching the two branches.
    let in_monorepo = std::path::Path::new("../../pkg/api/submit.proto").exists();

    // Only emit monorepo paths when they exist; cargo treats missing
    // rerun-if-changed paths as "always rerun", which would hurt consumers
    // of the published crate where ../../pkg/api/ is not present.
    if in_monorepo {
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
    let (armada_protos, armada_includes): (Vec<&str>, Vec<&str>) = if in_monorepo {
        // Input files are passed as include-relative names (e.g. "pkg/api/submit.proto")
        // rather than physical paths (e.g. "../../pkg/api/submit.proto"). This lets
        // protoc resolve them through the include list, which avoids the "Input is
        // shadowed" error that occurs when proto/pkg/api/ is also present on disk
        // (e.g. during `cargo package` verification).
        //
        // Include order: "proto" is first so that google/ and k8s.io/ imports always
        // resolve from the vendored copies in proto/ rather than anything that might
        // exist at the repo root. "../../" is second so that intra-Armada imports
        // (e.g. `import "pkg/api/health.proto"`) fall through to the monorepo source.
        (
            vec![
                "pkg/api/submit.proto",
                "pkg/api/event.proto",
                "pkg/api/job.proto",
                "pkg/api/health.proto",
            ],
            vec!["proto", "../../"],
        )
    } else {
        // Fallback: outside the monorepo and no pre-generated src/gen/ files.
        // Expects proto sources in proto/pkg/api/ (manually provided).
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

    // When ARMADA_GENERATE is set (release CI), copy the generated files into
    // src/gen/ so they can be bundled into the crates.io package.  Users who
    // download the crate then build without protoc via the early-return path above.
    if std::env::var("ARMADA_GENERATE").is_ok() {
        std::fs::create_dir_all(gen_dir)?;
        copy_rs_files(out_path, gen_dir)?;
    }

    Ok(())
}
