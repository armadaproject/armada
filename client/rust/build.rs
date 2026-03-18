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
    // NOTE: This probe assumes the crate lives exactly two directories below the
    // monorepo root (i.e. <root>/client/rust/). If the crate is relocated,
    // update these relative paths accordingly.
    //
    // Cached once so both the rerun-if-changed directives and the compile_protos
    // calls use the same decision, preventing a future refactor from accidentally
    // mismatching the two branches.
    let in_monorepo = std::path::Path::new("../../pkg/api/submit.proto").exists();

    // proto_root is the directory that contains k8s.io/, google/, etc.
    // In the monorepo it is the repo-root proto/ (populated by `mage BootstrapProto`).
    // Outside the monorepo it is proto/ relative to this crate (manually provided).
    let proto_root = if in_monorepo { "../../proto" } else { "proto" };

    // Without these directives Cargo's default is to re-run build.rs on every
    // source change. Restrict re-runs to changes that actually affect codegen.
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=ARMADA_GENERATE");
    // Only watch src/gen/ outside the monorepo (crates.io path). Cargo treats
    // missing rerun-if-changed paths as "always rerun", so emitting this in a
    // fresh monorepo clone (where src/gen/ never exists) would cause build.rs
    // to re-run on every build.
    if !in_monorepo {
        println!("cargo:rerun-if-changed=src/gen/");
    }

    if in_monorepo {
        // Narrow to the two subdirectories actually consumed by this build.
        // Watching all of ../../proto/ would trigger rebuilds on unrelated
        // proto changes elsewhere in the monorepo.
        println!("cargo:rerun-if-changed=../../proto/k8s.io/");
        println!("cargo:rerun-if-changed=../../proto/google/");
        println!("cargo:rerun-if-changed=../../pkg/api/submit.proto");
        println!("cargo:rerun-if-changed=../../pkg/api/event.proto");
        println!("cargo:rerun-if-changed=../../pkg/api/job.proto");
        println!("cargo:rerun-if-changed=../../pkg/api/health.proto");
    } else {
        // Outside the monorepo, expect third-party protos under proto/ relative
        // to this crate (manually provided).
        println!("cargo:rerun-if-changed=proto/");
    }

    let out_dir = std::env::var("OUT_DIR")?;
    let out_path = std::path::Path::new(&out_dir);
    let gen_dir = std::path::Path::new("src/gen");

    // Pre-generated files exist (published crate from crates.io): copy them to
    // OUT_DIR so that tonic::include_proto! resolves correctly, then return.
    // This path requires no protoc and no proto sources.
    // Guard on !in_monorepo so that monorepo contributors always compile from
    // proto sources and never accidentally use stale pre-generated files left
    // behind by a previous ARMADA_GENERATE=1 run.
    if !in_monorepo && gen_dir.join("api.rs").exists() {
        copy_rs_files(gen_dir, out_path)?;
        return Ok(());
    }

    // Pass 1: compile k8s protos to generate their Rust types.
    // No extern_path — we want these files emitted into OUT_DIR.
    let k8s_protos: Vec<String> = [
        "k8s.io/api/core/v1/generated.proto",
        "k8s.io/api/networking/v1/generated.proto",
        "k8s.io/apimachinery/pkg/api/resource/generated.proto",
        "k8s.io/apimachinery/pkg/apis/meta/v1/generated.proto",
        "k8s.io/apimachinery/pkg/runtime/generated.proto",
        "k8s.io/apimachinery/pkg/util/intstr/generated.proto",
    ]
    .iter()
    .map(|p| format!("{proto_root}/{p}"))
    .collect();

    tonic_build::configure()
        .build_client(false)
        .build_server(false)
        .compile_protos(&k8s_protos, &[proto_root])?;

    // Pass 2: compile Armada API protos.
    // extern_path tells prost that k8s types live in our module tree (from pass 1)
    // rather than generating duplicate inline copies inside api.rs.
    //
    // When building inside the monorepo the protos live at ../../pkg/api/ relative
    // to this manifest. When building from a crates.io package they are vendored
    // into proto/pkg/api/ (mirroring the import paths used inside the .proto files).
    //
    // Input files are passed as include-relative names (e.g. "pkg/api/submit.proto")
    // in the monorepo rather than physical paths (e.g. "../../pkg/api/submit.proto").
    // This lets protoc resolve them through the include list, which avoids the
    // "Input is shadowed" error that occurs when proto/pkg/api/ is also present on
    // disk (e.g. during `cargo package` verification).
    let armada_protos: Vec<String> = ["submit", "event", "job", "health"]
        .iter()
        .map(|name| {
            if in_monorepo {
                format!("pkg/api/{name}.proto")
            } else {
                format!("{proto_root}/pkg/api/{name}.proto")
            }
        })
        .collect();

    // Include order: proto_root is first so that google/ and k8s.io/ imports
    // always resolve from the bootstrapped proto directory rather than anything
    // else. "../../" is second (monorepo only) so that intra-Armada imports
    // (e.g. `import "pkg/api/health.proto"`) fall through to the monorepo source.
    let armada_includes: Vec<&str> = if in_monorepo {
        vec![proto_root, "../../"]
    } else {
        vec![proto_root]
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
        if gen_dir.exists() {
            std::fs::remove_dir_all(gen_dir)?;
        }
        std::fs::create_dir_all(gen_dir)?;
        copy_rs_files(out_path, gen_dir)?;
    }

    Ok(())
}
