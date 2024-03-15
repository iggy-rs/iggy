mod test_pat_create_command;
mod test_pat_delete_command;
mod test_pat_help_command;
mod test_pat_list_command;
// Disable tests due to missing keyring on macOS until #794 is implemented
#[cfg(not(target_os = "macos"))]
mod test_pat_login_options;
