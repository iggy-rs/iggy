use bcrypt::{hash, verify};

pub fn hash_password(password: &str) -> String {
    hash(password, 4).unwrap()
}

pub fn verify_password(password: &str, hash: &str) -> bool {
    verify(password, hash).unwrap_or(false)
}
