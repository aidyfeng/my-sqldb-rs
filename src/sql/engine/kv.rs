use crate::{error::Result, storage::{self, engine::Engine as StorageEngin}};

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
        todo!()
    }

    fn get_table(&self, table_name: String) -> Result<Option<crate::sql::schema::Table>> {
        todo!()
    }
}
