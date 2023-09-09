use uuid::Uuid;

pub fn get() -> u128 {
    Uuid::new_v4().to_u128_le()
}
