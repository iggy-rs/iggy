pub mod create_user;
pub mod delete_user;
pub mod login_user;
pub mod logout_user;
pub mod update_user;

const MAX_USERNAME_LENGTH: usize = 50;
const MIN_USERNAME_LENGTH: usize = 3;
const MAX_PASSWORD_LENGTH: usize = 100;
const MIN_PASSWORD_LENGTH: usize = 3;
