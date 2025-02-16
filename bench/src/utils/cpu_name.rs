use sysinfo::System;

pub fn append_cpu_name_lowercase(to: &mut String) {
    let mut sys = System::new();
    sys.refresh_all();

    let cpu = sys
        .cpus()
        .first()
        .map(|cpu| (cpu.brand().to_string(), cpu.frequency()))
        .unwrap_or_else(|| (String::from("unknown"), 0))
        .0
        .to_lowercase()
        .replace(' ', "_");

    to.push('_');
    to.push_str(&cpu);
}
