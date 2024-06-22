use bcrypt::{hash, verify};

pub fn hash_password(password: String) -> String {
    hash(password, 4).unwrap()
}

pub fn verify_password(password: String, hash: &str) -> bool {
    verify(password, hash).unwrap_or(false)
}
