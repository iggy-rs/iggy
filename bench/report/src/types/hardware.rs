use serde::{Deserialize, Serialize};
use sysinfo::System;

#[derive(Debug, Serialize, Deserialize, Clone, derive_new::new, PartialEq, Default)]
pub struct BenchmarkHardware {
    pub identifier: Option<String>,
    pub cpu_name: String,
    pub cpu_cores: usize,
    pub total_memory_mb: u64,
    pub os_name: String,
    pub os_version: String,
}

impl BenchmarkHardware {
    pub fn get_system_info_with_identifier(identifier: Option<String>) -> Self {
        let mut sys = System::new();
        sys.refresh_all();

        let cpu = sys
            .cpus()
            .first()
            .map(|cpu| (cpu.brand().to_string(), cpu.frequency()))
            .unwrap_or_else(|| (String::from("unknown"), 0));

        Self {
            identifier,
            cpu_name: cpu.0,
            cpu_cores: sys.cpus().len(),
            total_memory_mb: sys.total_memory() / 1024 / 1024,
            os_name: sysinfo::System::name().unwrap_or_else(|| String::from("unknown")),
            os_version: sysinfo::System::kernel_version()
                .unwrap_or_else(|| String::from("unknown")),
        }
    }
}
