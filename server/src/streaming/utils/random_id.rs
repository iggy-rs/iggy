use ulid::Ulid;
use uuid::Uuid;

pub fn get_uuid() -> u128 {
    Uuid::new_v4().to_u128_le()
}

pub fn get_ulid() -> Ulid {
    Ulid::new()
}
