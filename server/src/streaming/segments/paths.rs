pub const LOG_EXTENSION: &str = "log";
pub const INDEX_EXTENSION: &str = "index";
pub const TIME_INDEX_EXTENSION: &str = "timeindex";

pub fn get_log_path(path: &str) -> String {
    format!("{}.{}", path, LOG_EXTENSION)
}

pub fn get_index_path(path: &str) -> String {
    format!("{}.{}", path, INDEX_EXTENSION)
}

pub fn get_time_index_path(path: &str) -> String {
    format!("{}.{}", path, TIME_INDEX_EXTENSION)
}
