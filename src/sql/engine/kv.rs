use serde::{de::value, Deserialize, Serialize};

use crate::{error::{Error, Result}, storage::{self, engine::Engine as StorageEngin}};

use super::{Engine, Transaction};

pub struct KVEngine<E: StorageEngin> {
    pub kv: storage::mvcc::Mvcc<E>,
}

impl<E: StorageEngin> Clone for KVEngine<E> {
    fn clone(&self) -> Self {
        Self {
            kv: self.kv.clone(),
        }
    }
}

impl<E: StorageEngin> Engine for KVEngine<E> {
    type Transaction = KVTransaction<E>;

    fn begin(&self) -> Result<Self::Transaction> {
        Ok(Self::Transaction::new(self.kv.begin()?))
    }
}

//KVTransaction 定义,实际上对存储引擎MvccTransaction的封装
pub struct KVTransaction<E:StorageEngin> {
    txn: storage::mvcc::MvccTransaction<E>,
}

impl<E:StorageEngin> KVTransaction<E> {
    pub fn new(txn: storage::mvcc::MvccTransaction<E>) -> Self {
        Self { txn }
    }
}

impl<E:StorageEngin> Transaction for KVTransaction<E> {
    fn commit(&self) -> Result<()> {
        todo!()
    }

    fn rollback(&self) -> Result<()> {
        todo!()
    }

    fn create_row(&mut self, table: String, row: crate::sql::types::Row) -> Result<()> {
        todo!()
    }

    fn scan_table(&self, table: crate::sql::schema::Table) -> Result<Vec<crate::sql::types::Row>> {
        todo!()
    }

    fn create_table(&mut self, table: crate::sql::schema::Table) -> Result<()> {
        //判断表是否已经存在
        if self.get_table(table.name.clone())?.is_some(){
            return Err(Error::Internal(format!("table {} has already exists",table.name)))
        }

        //判断表的有效性
        if table.columns.is_empty(){
            return Err(Error::Internal(format!("table {} has no columns",table.name)))
        }

        let key = Key::Table(table.name.clone());
        let value = bincode::serialize(&table)?;

        self.txn.set(bincode::serialize(&key)?, value)
    }

    fn get_table(&self, table_name: String) -> Result<Option<crate::sql::schema::Table>> {
        let key = Key::Table(table_name);
        let v = self.txn
            .get(bincode::serialize(&key)?)?
            .map(|it| bincode::deserialize(&it)).transpose()?;
        Ok(v)
    }
}

#[derive(Debug,Serialize,Deserialize)]
enum Key {
    Table(String),
    Row(String,String)
}