// Disable tests due to missing keyring on macOS until #794 is implemented
#[cfg(not(target_os = "macos"))]
mod test_cli_session_scenario;
mod test_login_cmd;
mod test_login_command;
mod test_logout_cmd;
mod test_logout_command;
mod test_me_command;
mod test_ping_command;
mod test_stats_command;
