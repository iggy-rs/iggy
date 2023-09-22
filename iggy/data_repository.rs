use std::error::Error;

use crate::errors_repository::ErrorRepositoryEntry;

pub trait DataRepository<DataType> {
    fn insert(&self, value: DataType) -> Result<(), Box<dyn Error>>;
    fn fetch_all(&self) -> Result<Vec<DataType>, Box<dyn Error>>;
}

impl DataRepository<ErrorRepositoryEntry> for SledDb {
    fn insert(&self, value: ErrorRepositoryEntry) -> Result<(), Box<dyn Error>> {
        let serialized_value = rmp_serde::to_vec(&value).unwrap();
        let key = self.connection.generate_id()?.to_be_bytes();

        self.connection.insert(key, serialized_value)?;

        Ok(())
    }

    fn fetch_all(&self) -> Result<Vec<ErrorRepositoryEntry>, Box<dyn Error>> {
        let mut result: Vec<ErrorRepositoryEntry> = vec![];

        for key in self.connection.iter().keys() {
            let data = self.connection.get(key.unwrap())?;
            result.push(
                rmp_serde::from_slice::<ErrorRepositoryEntry>(&data.clone().unwrap()).unwrap(),
            );
        }

        Ok(result)
    }
}

pub struct SledDb {
    pub connection: sled::Db,
}
