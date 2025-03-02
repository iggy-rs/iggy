// Disable tests due to missing keyring on macOS until #794 is implemented and skip for musl targets
// due to missing keyring support while running tests under cross
#[cfg(not(any(target_os = "macos", target_env = "musl")))]
mod test_cli_session_scenario;
#[cfg(not(any(target_os = "macos", target_env = "musl")))]
mod test_login_cmd;
mod test_login_command;
#[cfg(not(any(target_os = "macos", target_env = "musl")))]
mod test_logout_cmd;
mod test_logout_command;
#[cfg(not(any(target_os = "macos", target_env = "musl")))]
mod test_me_command;
mod test_ping_command;
mod test_snapshot_cmd;
mod test_stats_command;
