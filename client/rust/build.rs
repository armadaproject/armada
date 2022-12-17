use std::{io::Write, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let gen_dir = PathBuf::from("./src/gen");
    if !gen_dir.exists() {
        std::fs::create_dir_all(&gen_dir)?;
    }

    let gen_module = gen_dir.join("mod.rs");
    if !gen_module.exists() {
        let mut f = std::fs::File::create(gen_module)?;
        f.write_all(b"pub mod armada;")?;
    }
    tonic_build::configure()
        .build_client(true)
        .compile_well_known_types(false)
        .message_attribute(".", "#[derive(derive_builder::Builder)]")
        .include_file("armada.rs")
        .out_dir("./src/gen")
        .compile(
            &[
                "./../../proto/armada/event.proto",
                "./../../proto/armada/queue.proto",
                "./../../proto/armada/submit.proto",
                "./../../proto/armada/usage.proto",
                "./../../proto/armada/health.proto",
                "./../../proto/google/api/annotations.proto",
                "./../../proto/google/api/http.proto",
                "./../../proto/github.com/gogo/protobuf/gogoproto/gogo.proto",
                "./../../proto/k8s.io/api/core/v1/generated.proto",
                "./../../proto/k8s.io/apimachinery/pkg/api/resource/generated.proto",
                "./../../proto/k8s.io/apimachinery/pkg/apis/meta/v1/generated.proto",
                "./../../proto/k8s.io/apimachinery/pkg/runtime/generated.proto",
                "./../../proto/k8s.io/apimachinery/pkg/runtime/schema/generated.proto",
                "./../../proto/k8s.io/apimachinery/pkg/util/intstr/generated.proto",
                "./../../proto/k8s.io/api/networking/v1/generated.proto",
            ],
            &["./../../proto/"],
        )?;

    Ok(())
}
