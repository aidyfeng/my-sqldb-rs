use serde::{Deserialize, Serialize};

use crate::{
    error::{Error, Result},
    sql::types::{Row, Value},
    storage::{self, engine::Engine as StorageEngin},
};

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

impl<E: StorageEngin> KVEngine<E> {
    pub fn new(engine: E) -> Self {
        Self {
            kv: storage::mvcc::Mvcc::new(engine),
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
pub struct KVTransaction<E: StorageEngin> {
    txn: storage::mvcc::MvccTransaction<E>,
}

impl<E: StorageEngin> KVTransaction<E> {
    pub fn new(txn: storage::mvcc::MvccTransaction<E>) -> Self {
        Self { txn }
    }
}

impl<E: StorageEngin> Transaction for KVTransaction<E> {
    fn commit(&self) -> Result<()> {
        self.txn.commit()
    }

    fn rollback(&self) -> Result<()> {
        self.txn.rollback()
    }

    fn create_row(&mut self, table_name: String, row: Row) -> Result<()> {
        let table = self.must_get_table(table_name.clone())?;

        //校验行可靠性
        for (i, col) in table.columns.iter().enumerate() {
            match row[i].datatype() {
                None if col.nullable => {}
                None => {
                    return Err(Error::Internal(format!(
                        "column {} can not be null",
                        col.name
                    )))
                }
                Some(datatype) if datatype != col.datatype => {
                    return Err(Error::Internal(format!(
                        "column {} type mismatch",
                        col.name
                    )))
                }
                _ => {}
            }
        }

        //存放数据
        //暂时以第一列作为主键, 一行的唯一标识
        let id = Key::Row(table_name.clone(), row[0].clone());
        let value = bincode::serialize(&row)?;
        self.txn.set(bincode::serialize(&id)?, value)?;
        Ok(())
    }

    fn scan_table(&self, table_name: String) -> Result<Vec<Row>> {
        let prefix = KeyPrefix::Row(table_name);
        let results = self.txn.scan_prefix(bincode::serialize(&prefix)?)?;
        let mut rows = Vec::new();
        for result in results {
            let row = bincode::deserialize(&result.value)?;
            rows.push(row);
        }
        Ok(rows)
    }

    fn create_table(&mut self, table: crate::sql::schema::Table) -> Result<()> {
        //判断表是否已经存在
        if self.get_table(table.name.clone())?.is_some() {
            return Err(Error::Internal(format!(
                "table {} has already exists",
                table.name
            )));
        }

        //判断表的有效性
        if table.columns.is_empty() {
            return Err(Error::Internal(format!(
                "table {} has no columns",
                table.name
            )));
        }

        let key = Key::Table(table.name.clone());
        let value = bincode::serialize(&table)?;

        self.txn.set(bincode::serialize(&key)?, value)
    }

    fn get_table(&self, table_name: String) -> Result<Option<crate::sql::schema::Table>> {
        let key = Key::Table(table_name);
        let v = self
            .txn
            .get(bincode::serialize(&key)?)?
            .map(|it| bincode::deserialize(&it))
            .transpose()?;
        Ok(v)
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum Key {
    Table(String),
    Row(String, Value),
}

#[derive(Debug, Serialize, Deserialize)]
enum KeyPrefix {
    Table,
    Row(String),
}

#[cfg(test)]
mod tests {
    use crate::{error::Result, sql::engine::Engine, storage::memory::MemoryEngine};

    use super::KVEngine;

    #[test]
    fn test_create_table() -> Result<()> {
        let kvengine = KVEngine::new(MemoryEngine::new());
        let mut s = kvengine.session()?;
        s.execute("create table t1 (a int , b text default 'vv', c integer default 100);")?;
        s.execute("insert into t1 values(1,'a',1);")?;
        s.execute("insert into t1 values(2,'b',2);")?;
        s.execute("insert into t1(a) values(3);")?;
        let v1 = s.execute("select * from t1;")?;
        println!("{:?}", v1);
        Ok(())
    }
}
