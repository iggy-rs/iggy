use crate::cli::common::command::IggyCmdCommand;
use assert_cmd::assert::Assert;
use predicates::str::diff;

// Clap long help messages contain set of white spaces between short summary and detailed
// description. In case of IDE removes help message verification this variable is being used to
// format expected message and prevent from automatic removal of white spaces.
pub(crate) const CLAP_INDENT: &str = "          ";

#[cfg(windows)]
pub(crate) const USAGE_PREFIX: &str = "Usage: iggy.exe";

#[cfg(not(windows))]
pub(crate) const USAGE_PREFIX: &str = "Usage: iggy";

pub(crate) struct TestHelpCmd {
    help_command: Vec<String>,
    expected_output: String,
}

impl TestHelpCmd {
    pub(crate) fn new(command: Vec<impl Into<String>>, expected_output: String) -> Self {
        let help_command = command.into_iter().map(|s| s.into()).collect();
        Self {
            help_command,
            expected_output,
        }
    }

    pub(super) fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new().args(self.help_command.clone())
    }

    pub(super) fn verify_command(&self, command_state: Assert) {
        command_state
            .success()
            .stdout(diff(self.expected_output.clone()));
    }
}
