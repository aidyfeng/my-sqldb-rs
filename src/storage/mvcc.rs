use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    sync::{Arc, Mutex, MutexGuard}, u64,
};

use super::engine::{self, Engine};

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

impl TransactionState {
    fn is_visible(&self,version:Version) -> bool {
        if self.active_versions.contains(&version) {
            false
        }else {
            version <= self.version
        }
    }
}



#[derive(Debug, Serialize, Deserialize)]
pub enum MvccKey {
    NextVersion,
    TxnActive(Version),
    TxnWrite(Version,Vec<u8>),
    Version(Vec<u8>, Version),
}

//Version key1-101, key2-102

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

    fn write(&self, key: Vec<u8>, value: Option<Vec<u8>>) -> Result<()> {
        //获取存储引擎
        let mut engine = self.engine.lock()?;

        //检测冲突
        //当前活跃列表 3  4  5
        //当前事务 6
        //key1-3 key2-4 key3-5
        let from = MvccKey::Version(key.clone(), self.state.active_versions.iter().min().copied().unwrap_or(self.state.version +1)).encode();
        let to = MvccKey::Version(key.clone(), u64::MAX).encode();
        //只需判断最后一个版本号
        //1. key按顺序排列, 扫描出的结果从小到大
        //2. 加入有的事务修改了数据, 比如10, 如果当前事务6修改, 那么冲突了
        //3. 如果是当前事务修改了这个key,比如4,那么事务5就不可能修改这个key
        if let Some((k,_)) = engine.scan(from..=to).last().transpose()? {
            match MvccKey::decode(k.clone())? {
                MvccKey::Version(_, version) => {
                    //检测version是否可见
                    if !self.state.is_visible(version) {
                        return Err(Error::WriteConflict);
                    }
                },
                _ => {
                    return Err(Error::Internal(format!(
                        "unexpected key: {:?}",
                        String::from_utf8(k)
                    )))
                },
            }
        }

        //记录这个version,写入哪些key, 用于回滚事务
        engine.set(MvccKey::TxnWrite(self.state.version,key.clone()).encode(), vec![])?;

        //写入实际的key,value数据

        Ok(())


    }
}

pub struct ScanResult {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}
