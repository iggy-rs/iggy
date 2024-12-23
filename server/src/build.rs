use std::error;
use std::path::PathBuf;
use vergen_git2::{BuildBuilder, CargoBuilder, Emitter, Git2Builder, RustcBuilder, SysinfoBuilder};

fn main() -> Result<(), Box<dyn error::Error>> {
    if option_env!("IGGY_CI_BUILD") == Some("true") {
        Emitter::default()
            .add_instructions(&BuildBuilder::all_build()?)?
            .add_instructions(&CargoBuilder::all_cargo()?)?
            .add_instructions(&Git2Builder::all_git()?)?
            .add_instructions(&RustcBuilder::all_rustc()?)?
            .add_instructions(&SysinfoBuilder::all_sysinfo()?)?
            .emit()?;

        let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..");

        println!(
            "cargo:rerun-if-changed={}",
            workspace_root
                .join("configs")
                .canonicalize()
                .unwrap()
                .display()
        );
    } else {
        println!("cargo:info=Skipping build script because CI environment variable IGGY_CI_BUILD is not set to 'true'");
    }

    Ok(())
}
