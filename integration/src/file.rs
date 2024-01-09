use std::ffi::OsStr;
use std::path::{Path, PathBuf};

pub fn get_root_path() -> PathBuf {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set!");
    PathBuf::from(manifest_dir)
}

pub fn file_exists<S>(file_path: &S) -> bool
where
    S: AsRef<OsStr> + ?Sized,
{
    Path::new(file_path).is_file()
}
