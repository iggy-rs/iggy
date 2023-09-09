use std::error::{self};
use vergen::EmitBuilder;

fn main() -> Result<(), Box<dyn error::Error>> {
    EmitBuilder::builder()
        .all_build()
        .all_cargo()
        .all_git()
        .all_rustc()
        .emit()?;
    Ok(())
}
