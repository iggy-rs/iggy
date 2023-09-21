use std::error::Error;

use crate::ErrorCode;

pub trait DataRepository<DataType> {
    fn insert(&self, value: DataType) -> Result<(), Box<dyn Error>>;
    fn fetch_all(&self) -> Result<Vec<ErrorCode>, Box<dyn Error>>;
}

impl DataRepository<ErrorCode> for SledDb {
    fn insert(&self, value: ErrorCode) -> Result<(), Box<dyn Error>> {
        let serialized_value = rmp_serde::to_vec(&value).unwrap();
        let key = self.connection.generate_id()?.to_be_bytes();

        self.connection.insert(key, serialized_value)?;

        Ok(())
    }

    fn fetch_all(&self) -> Result<Vec<ErrorCode>, Box<dyn Error>> {
        let mut result: Vec<ErrorCode> = vec![];

        for key in self.connection.iter().keys() {
            let data = self.connection.get(key.unwrap())?;
            result.push(rmp_serde::from_slice::<ErrorCode>(
                &data.clone().unwrap()).unwrap()
            );
        }

        Ok(result)
    }
}

pub struct SledDb {
    pub connection: sled::Db,
}
