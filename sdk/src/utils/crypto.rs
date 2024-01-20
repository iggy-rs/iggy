use crate::error::IggyError;
use crate::utils::text;
use aes_gcm::aead::generic_array::GenericArray;
use aes_gcm::aead::{Aead, OsRng};
use aes_gcm::{AeadCore, Aes256Gcm, KeyInit};
use std::fmt::Debug;

pub trait Encryptor: Send + Sync + Debug {
    fn encrypt(&self, data: &[u8]) -> Result<Vec<u8>, IggyError>;
    fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>, IggyError>;
}

pub struct Aes256GcmEncryptor {
    cipher: Aes256Gcm,
}

unsafe impl Send for Aes256GcmEncryptor {}
unsafe impl Sync for Aes256GcmEncryptor {}

impl Debug for Aes256GcmEncryptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Encryptor").finish()
    }
}

impl Aes256GcmEncryptor {
    pub fn new(key: &[u8]) -> Result<Self, IggyError> {
        if key.len() != 32 {
            return Err(IggyError::InvalidEncryptionKey);
        }
        Ok(Self {
            cipher: Aes256Gcm::new(GenericArray::from_slice(key)),
        })
    }

    pub fn from_base64_key(key: &str) -> Result<Self, IggyError> {
        Self::new(&text::from_base64_as_bytes(key)?)
    }
}

impl Encryptor for Aes256GcmEncryptor {
    fn encrypt(&self, data: &[u8]) -> Result<Vec<u8>, IggyError> {
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let encrypted_data = self.cipher.encrypt(&nonce, data);
        if encrypted_data.is_err() {
            return Err(IggyError::CannotEncryptData);
        }
        let payload = [&nonce, encrypted_data.unwrap().as_slice()].concat();
        Ok(payload)
    }

    fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>, IggyError> {
        let nonce = GenericArray::from_slice(&data[0..12]);
        let payload = self.cipher.decrypt(nonce, &data[12..]);
        if payload.is_err() {
            return Err(IggyError::CannotDecryptData);
        }
        Ok(payload.unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_the_same_key_data_should_be_encrypted_and_decrypted_correctly() {
        let key = [1; 32];
        let encryptor = Aes256GcmEncryptor::new(&key).unwrap();
        let data = b"Hello World!";
        let encrypted_data = encryptor.encrypt(data);
        assert!(encrypted_data.is_ok());
        let encrypted_data = encrypted_data.unwrap();
        let decrypted_data = encryptor.decrypt(&encrypted_data);
        assert!(decrypted_data.is_ok());
        let decrypted_data = decrypted_data.unwrap();
        assert_eq!(data, decrypted_data.as_slice());
    }

    #[test]
    fn given_the_invalid_key_data_should_not_be_decrypted_correctly() {
        let first_key = [1; 32];
        let second_key = [2; 32];
        let first_encryptor = Aes256GcmEncryptor::new(&first_key).unwrap();
        let second_encryptor = Aes256GcmEncryptor::new(&second_key).unwrap();
        let data = b"Hello World!";
        let encrypted_data = first_encryptor.encrypt(data);
        assert!(encrypted_data.is_ok());
        let encrypted_data = encrypted_data.unwrap();
        let decrypted_data = second_encryptor.decrypt(&encrypted_data);
        assert!(decrypted_data.is_err());
        let error = decrypted_data.err().unwrap();
        assert_eq!(error.as_code(), IggyError::CannotDecryptData.as_code());
    }
}
