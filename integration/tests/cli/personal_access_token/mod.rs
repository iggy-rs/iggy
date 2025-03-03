mod test_pat_create_command;
mod test_pat_delete_command;
mod test_pat_help_command;
mod test_pat_list_command;
// Disable tests due to missing keyring on macOS until #794 is implemented and skip for musl targets
// due to missing keyring support while running tests under cross
#[cfg(not(any(target_os = "macos", target_env = "musl")))]
mod test_pat_login_options;
