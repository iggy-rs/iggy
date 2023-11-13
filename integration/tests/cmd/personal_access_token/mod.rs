mod test_pat_create_command;
mod test_pat_delete_command;
mod test_pat_help_command;
mod test_pat_list_command;
// Disable tests due to missing keyring on arm and aarch64 until #294 is implemented
#[cfg(not(any(target_arch = "aarch64", target_arch = "arm")))]
mod test_pat_login_options;
