use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    sync::{Arc, Mutex, MutexGuard},
};

use super::engine::Engine;

pub type Version = u64;

pub struct Mvcc<E: Engine> {
    engine: Arc<Mutex<E>>,
}

impl<E: Engine> Clone for Mvcc<E> {
    fn clone(&self) -> Self {
        Self {
            engine: self.engine.clone(),
        }
    }
}

impl<E: Engine> Mvcc<E> {
    pub fn new(eng: E) -> Self {
        Self {
            engine: Arc::new(Mutex::new(eng)),
        }
    }

    pub fn begin(&self) -> Result<MvccTransaction<E>> {
        MvccTransaction::begin(self.engine.clone())
    }
}

pub struct MvccTransaction<E: Engine> {
    engine: Arc<Mutex<E>>,
    state: TransactionState,
}

pub struct TransactionState {
    //当前版本号
    pub version: Version,

    //当前事务下活跃事务列表
    pub active_versions: HashSet<Version>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum MvccKey {
    NextVersion,
    TxnActive(Version),
}

impl MvccKey {
    pub fn encode(&self) -> Vec<u8> {
        bincode::serialize(&self).unwrap()
    }

    pub fn decode(data: Vec<u8>) -> Result<MvccKey> {
        Ok(bincode::deserialize(&data)?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum MvccKeyPrefix {
    NextVersion,
    TxnActive,
}

impl MvccKeyPrefix {
    pub fn encode(&self) -> Vec<u8> {
        bincode::serialize(&self).unwrap()
    }
}

impl<E: Engine> MvccTransaction<E> {
    pub fn begin(eng: Arc<Mutex<E>>) -> Result<Self> {
        //获取存储引擎
        let mut engine = eng.lock()?;
        //获取版本号
        let next_version = match engine.get(MvccKey::NextVersion.encode())? {
            Some(val) => bincode::deserialize(&val)?,
            None => 1,
        };

        engine.set(
            MvccKey::NextVersion.encode(),
            bincode::serialize(&(next_version + 1))?,
        )?;

        //获取当前活跃的事务列表
        let active_versions = Self::scan_active(&mut engine)?;

        //当前事务加入到活跃事务列表
        engine.set(MvccKey::TxnActive(next_version).encode(), vec![])?;

        Ok(Self {
            engine: eng.clone(),
            state: TransactionState {
                version: next_version,
                active_versions,
            },
        })
    }

    pub fn commit(&self) -> Result<()> {
        Ok(())
    }

    pub fn rollback(&self) -> Result<()> {
        Ok(())
    }

    pub fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let mut eng = self.engine.lock()?;
        eng.set(key, value)
    }

    pub fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let mut eng = self.engine.lock()?;
        eng.get(key)
    }

    pub fn scan_prefix(&self, prefix: Vec<u8>) -> Result<Vec<ScanResult>> {
        let mut eng = self.engine.lock()?;
        let mut iter = eng.scan_prefix(prefix);
        let mut result = Vec::new();
        while let Some((key, value)) = iter.next().transpose()? {
            result.push(ScanResult { key, value });
        }
        Ok(result)
    }

    fn scan_active(engine: &mut MutexGuard<E>) -> Result<HashSet<Version>> {
        let mut active_versions = HashSet::new();
        let mut iter = engine.scan_prefix(MvccKeyPrefix::TxnActive.encode());
        while let Some((key, _)) = iter.next().transpose()? {
            match MvccKey::decode(key.clone())? {
                MvccKey::TxnActive(version) => {
                    active_versions.insert(version);
                }
                _ => {
                    return Err(Error::Internal(format!(
                        "unexpected key: {:?}",
                        String::from_utf8(key)
                    )))
                }
            }
        }

        Ok(active_versions)
    }
}

pub struct ScanResult {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}
