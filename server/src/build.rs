use std::error;
use vergen::EmitBuilder;

fn main() -> Result<(), Box<dyn error::Error>> {
    if option_env!("IGGY_CI_BUILD") == Some("true") {
        EmitBuilder::builder()
            .all_build()
            .all_cargo()
            .all_git()
            .all_rustc()
            .emit()?;
    } else {
        println!("cargo:info=Skipping build script because CI environment variable IGGY_CI_BUILD is not set to 'true'");
    }
    Ok(())
}
