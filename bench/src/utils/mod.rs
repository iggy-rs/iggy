use std::{fs, path::Path};

pub mod client_factory;
pub mod server_starter;
pub mod server_version;

pub fn copy_directory_contents(source_dir: &Path, target_dir: &Path) -> std::io::Result<()> {
    for entry in fs::read_dir(source_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            let file_name = path.file_name().unwrap();
            let target_path = target_dir.join(file_name);
            fs::copy(&path, &target_path)?;
        }
    }
    Ok(())
}
