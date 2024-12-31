use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashSet},
    sync::{Arc, Mutex, MutexGuard},
};

use super::{
    engine::Engine,
    keycode::{deserialize_key, serialize_key},
};

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
    fn is_visible(&self, version: Version) -> bool {
        if self.active_versions.contains(&version) {
            false
        } else {
            version <= self.version
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum MvccKey {
    NextVersion,
    TxnActive(Version),
    TxnWrite(Version, #[serde(with = "serde_bytes")] Vec<u8>),
    Version(#[serde(with = "serde_bytes")] Vec<u8>, Version),
}

//Version key1-101, key2-102

impl MvccKey {
    pub fn encode(&self) -> Result<Vec<u8>> {
        // bincode::serialize(&self).unwrap()
        serialize_key(&self)
    }

    pub fn decode(data: Vec<u8>) -> Result<MvccKey> {
        // Ok(bincode::deserialize(&data)?)
        deserialize_key(&data)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum MvccKeyPrefix {
    NextVersion,
    TxnActive,
    TxnWrite(Version),
    Version(#[serde(with = "serde_bytes")] Vec<u8>),
}

impl MvccKeyPrefix {
    pub fn encode(&self) -> Result<Vec<u8>> {
        // bincode::serialize(&self).unwrap()
        serialize_key(&self)
    }
}

impl<E: Engine> MvccTransaction<E> {
    pub fn begin(eng: Arc<Mutex<E>>) -> Result<Self> {
        //获取存储引擎
        let mut engine = eng.lock()?;
        //获取版本号
        let next_version = match engine.get(MvccKey::NextVersion.encode()?)? {
            Some(val) => bincode::deserialize(&val)?,
            None => 1,
        };

        engine.set(
            MvccKey::NextVersion.encode()?,
            bincode::serialize(&(next_version + 1))?,
        )?;

        //获取当前活跃的事务列表
        let active_versions = Self::scan_active(&mut engine)?;

        //当前事务加入到活跃事务列表
        engine.set(MvccKey::TxnActive(next_version).encode()?, vec![])?;

        Ok(Self {
            engine: eng.clone(),
            state: TransactionState {
                version: next_version,
                active_versions,
            },
        })
    }

    pub fn commit(&self) -> Result<()> {
        let mut engine = self.engine.lock()?;

        let mut delete_keys = Vec::new();

        //找到这个事务的TxnWrite信息,并删除
        let mut iter = engine.scan_prefix(MvccKeyPrefix::TxnWrite(self.state.version).encode()?);
        while let Some((key, _)) = iter.next().transpose()? {
            delete_keys.push(key);
        }

        drop(iter);

        for key in delete_keys {
            engine.delete(key)?;
        }

        //删除活跃事务列表
        engine.delete(MvccKey::TxnActive(self.state.version).encode()?)
    }

    pub fn rollback(&self) -> Result<()> {
        let mut engine = self.engine.lock()?;

        let mut delete_keys = Vec::new();

        //找到这个事务的TxnWrite信息,并删除
        let mut iter = engine.scan_prefix(MvccKeyPrefix::TxnWrite(self.state.version).encode()?);
        while let Some((key, _)) = iter.next().transpose()? {
            match MvccKey::decode(key.clone())? {
                //MvccKey:Version信息也需要一并删掉
                MvccKey::TxnWrite(_, raw_key) => {
                    delete_keys.push(MvccKey::Version(raw_key, self.state.version).encode()?);
                }
                _ => {
                    return Err(Error::Internal(format!(
                        "unexpected key: {:?}",
                        String::from_utf8(key)
                    )))
                }
            }
            delete_keys.push(key);
        }

        drop(iter);

        for key in delete_keys {
            engine.delete(key)?;
        }

        //删除活跃事务列表
        engine.delete(MvccKey::TxnActive(self.state.version).encode()?)
    }

    pub fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.write_inner(key, Some(value))
    }

    pub fn delete(&self, key: Vec<u8>) -> Result<()> {
        self.write_inner(key, None)
    }

    pub fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let mut engine = self.engine.lock()?;
        //如果version : 9
        //扫描version的范围 0..=9
        let from = MvccKey::Version(key.clone(), 0).encode()?;
        let to = MvccKey::Version(key.clone(), self.state.version).encode()?;
        let mut iter = engine.scan(from..=to).rev();
        //从最新版本开始读, 找到最新可见的版本
        while let Some((key, value)) = iter.next().transpose()? {
            match MvccKey::decode(key.clone())? {
                MvccKey::Version(_, version) => {
                    if self.state.is_visible(version) {
                        return Ok(bincode::deserialize(&value)?);
                    }
                }
                _ => {
                    return Err(Error::Internal(format!(
                        "unexpected key: {:?}",
                        String::from_utf8(key)
                    )))
                }
            }
        }

        Ok(None)
    }

    pub fn scan_prefix(&self, prefix: Vec<u8>) -> Result<Vec<ScanResult>> {
        let mut eng = self.engine.lock()?;
        let mut enc_prefix = MvccKeyPrefix::Version(prefix).encode()?;

        //原始值       编码后
        //97,98,99 ->  97,98,99,0,0
        //前缀原始值     前缀编码后
        //97,98  ->     97,98,0,0
        //去掉最后的[0,0] 后缀
        enc_prefix.truncate(enc_prefix.len() - 2);
        let mut iter = eng.scan_prefix(enc_prefix);
        let mut btree_map = BTreeMap::new();
        while let Some((key, value)) = iter.next().transpose()? {
            match MvccKey::decode(key.clone())? {
                MvccKey::Version(raw_key, version) => {
                    if self.state.is_visible(version) {
                        match bincode::deserialize(&value)? {
                            Some(raw_value) => btree_map.insert(raw_key, raw_value),
                            None => btree_map.remove(&raw_key),
                        };
                    }
                }
                _ => {
                    return Err(Error::Internal(format!(
                        "Unexpected key {:?}",
                        String::from_utf8(key)
                    )));
                }
            }
        }
        let result = btree_map
            .into_iter()
            .map(|(key, value)| ScanResult { key, value })
            .collect();
        Ok(result)
    }

    fn scan_active(engine: &mut MutexGuard<E>) -> Result<HashSet<Version>> {
        let mut active_versions = HashSet::new();
        let mut iter = engine.scan_prefix(MvccKeyPrefix::TxnActive.encode()?);
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

    fn write_inner(&self, key: Vec<u8>, value: Option<Vec<u8>>) -> Result<()> {
        //获取存储引擎
        let mut engine = self.engine.lock()?;

        //检测冲突
        //当前活跃列表 3  4  5
        //当前事务 6
        //key1-3 key2-4 key3-5
        let from = MvccKey::Version(
            key.clone(),
            self.state
                .active_versions
                .iter()
                .min()
                .copied()
                .unwrap_or(self.state.version + 1),
        )
        .encode()?;
        let to = MvccKey::Version(key.clone(), u64::MAX).encode()?;
        //只需判断最后一个版本号
        //1. key按顺序排列, 扫描出的结果从小到大
        //2. 加入有的事务修改了数据, 比如10, 如果当前事务6修改, 那么冲突了
        //3. 如果是当前事务修改了这个key,比如4,那么事务5就不可能修改这个key
        if let Some((k, _)) = engine.scan(from..=to).last().transpose()? {
            match MvccKey::decode(k.clone())? {
                MvccKey::Version(_, version) => {
                    //检测version是否可见
                    if !self.state.is_visible(version) {
                        return Err(Error::WriteConflict);
                    }
                }
                _ => {
                    return Err(Error::Internal(format!(
                        "unexpected key: {:?}",
                        String::from_utf8(k)
                    )))
                }
            }
        }

        //记录这个version,写入哪些key, 用于回滚事务
        engine.set(
            MvccKey::TxnWrite(self.state.version, key.clone()).encode()?,
            vec![],
        )?;

        //写入实际的key,value数据
        engine.set(
            MvccKey::Version(key.clone(), self.state.version).encode()?,
            bincode::serialize(&value)?,
        )?;

        Ok(())
    }
}

#[derive(Debug, PartialEq)]
pub struct ScanResult {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use std::{fs::remove_dir_all, result};

    use crate::{
        error::{self, Error, Result},
        storage::{disk::DiskEngine, engine::Engine, memory::MemoryEngine, mvcc::ScanResult},
    };

    use super::Mvcc;

    fn get(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"key1".to_vec(), b"val1".to_vec())?;
        tx.set(b"key2".to_vec(), b"val2".to_vec())?;
        tx.set(b"key2".to_vec(), b"val3".to_vec())?;
        tx.set(b"key3".to_vec(), b"val4".to_vec())?;
        tx.delete(b"key3".to_vec())?;
        tx.commit()?;

        let tx1 = mvcc.begin()?;
        assert_eq!(tx1.get(b"key1".to_vec())?, Some(b"val1".to_vec()));
        assert_eq!(tx1.get(b"key2".to_vec())?, Some(b"val3".to_vec()));
        assert_eq!(tx1.get(b"key3".to_vec())?, None);

        Ok(())
    }

    #[test]
    fn test_get() -> Result<()> {
        get(MemoryEngine::new())?;

        let p: std::path::PathBuf = tempfile::tempdir()?.into_path().join("sqldb-log");
        get(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    fn get_isolation(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"key1".to_vec(), b"val1".to_vec())?;
        tx.set(b"key2".to_vec(), b"val2".to_vec())?;
        tx.set(b"key2".to_vec(), b"val3".to_vec())?;
        tx.set(b"key3".to_vec(), b"val4".to_vec())?;
        tx.delete(b"key3".to_vec())?;
        tx.commit()?;

        let tx1 = mvcc.begin()?;
        tx1.set(b"key1".to_vec(), b"val2".to_vec())?;

        let tx2 = mvcc.begin()?;

        let tx3 = mvcc.begin()?;
        tx3.set(b"key2".to_vec(), b"val4".to_vec())?;
        tx3.delete(b"key3".to_vec())?;
        tx3.commit()?;

        assert_eq!(tx2.get(b"key1".to_vec())?, Some(b"val1".to_vec()));
        assert_eq!(tx2.get(b"key2".to_vec())?, Some(b"val3".to_vec()));
        assert_eq!(tx2.get(b"key3".to_vec())?, None);

        Ok(())
    }

    #[test]
    fn test_get_isalation() -> Result<()> {
        get_isolation(MemoryEngine::new())?;

        let p: std::path::PathBuf = tempfile::tempdir()?.into_path().join("sqldb-log");
        get_isolation(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    fn scan_prefix(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"aabb".to_vec(), b"val1".to_vec())?;
        tx.set(b"abcc".to_vec(), b"val2".to_vec())?;
        tx.set(b"bbaa".to_vec(), b"val3".to_vec())?;
        tx.set(b"acca".to_vec(), b"val4".to_vec())?;
        tx.set(b"aaca".to_vec(), b"val5".to_vec())?;
        tx.set(b"bcca".to_vec(), b"val6".to_vec())?;
        tx.commit()?;

        let tx1 = mvcc.begin()?;
        let iter1 = tx1.scan_prefix(b"aa".to_vec())?;
        assert_eq!(
            iter1,
            vec![
                ScanResult {
                    key: b"aabb".to_vec(),
                    value: b"val1".to_vec()
                },
                ScanResult {
                    key: b"aaca".to_vec(),
                    value: b"val5".to_vec()
                }
            ]
        );

        let iter2 = tx1.scan_prefix(b"a".to_vec())?;
        assert_eq!(
            iter2,
            vec![
                ScanResult {
                    key: b"aabb".to_vec(),
                    value: b"val1".to_vec()
                },
                ScanResult {
                    key: b"aaca".to_vec(),
                    value: b"val5".to_vec()
                },
                ScanResult {
                    key: b"abcc".to_vec(),
                    value: b"val2".to_vec()
                },
                ScanResult {
                    key: b"acca".to_vec(),
                    value: b"val4".to_vec()
                }
            ]
        );

        let iter3 = tx1.scan_prefix(b"bcca".to_vec())?;
        assert_eq!(
            iter3,
            vec![ScanResult {
                key: b"bcca".to_vec(),
                value: b"val6".to_vec()
            },]
        );

        Ok(())
    }

    #[test]
    fn test_scan_prefix() -> Result<()> {
        scan_prefix(MemoryEngine::new())?;

        let p: std::path::PathBuf = tempfile::tempdir()?.into_path().join("sqldb-log");
        scan_prefix(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    fn scan_isolation(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"aabb".to_vec(), b"val1".to_vec())?;
        tx.set(b"abcc".to_vec(), b"val2".to_vec())?;
        tx.set(b"bbaa".to_vec(), b"val3".to_vec())?;
        tx.set(b"acca".to_vec(), b"val4".to_vec())?;
        tx.set(b"aaca".to_vec(), b"val5".to_vec())?;
        tx.set(b"bcca".to_vec(), b"val6".to_vec())?;
        tx.commit()?;

        let tx1 = mvcc.begin()?;
        let tx2 = mvcc.begin()?;
        tx2.set(b"acca".to_vec(), b"val4-1".to_vec())?;
        tx2.set(b"aabb".to_vec(), b"val1-1".to_vec())?;

        let tx3 = mvcc.begin()?;
        tx3.set(b"bbaa".to_vec(), b"val3-1".to_vec())?;
        tx3.delete(b"bcca".to_vec())?;
        tx3.commit()?;

        let iter1 = tx1.scan_prefix(b"aa".to_vec())?;
        assert_eq!(
            iter1,
            vec![
                ScanResult {
                    key: b"aabb".to_vec(),
                    value: b"val1".to_vec()
                },
                ScanResult {
                    key: b"aaca".to_vec(),
                    value: b"val5".to_vec()
                },
            ]
        );

        let iter2 = tx1.scan_prefix(b"a".to_vec())?;
        assert_eq!(
            iter2,
            vec![
                ScanResult {
                    key: b"aabb".to_vec(),
                    value: b"val1".to_vec()
                },
                ScanResult {
                    key: b"aaca".to_vec(),
                    value: b"val5".to_vec()
                },
                ScanResult {
                    key: b"abcc".to_vec(),
                    value: b"val2".to_vec()
                },
                ScanResult {
                    key: b"acca".to_vec(),
                    value: b"val4".to_vec()
                },
            ]
        );

        let iter3 = tx1.scan_prefix(b"bcca".to_vec())?;
        assert_eq!(
            iter3,
            vec![ScanResult {
                key: b"bcca".to_vec(),
                value: b"val6".to_vec()
            },]
        );

        Ok(())
    }

    #[test]
    fn test_scan_isolation() -> Result<()> {
        scan_isolation(MemoryEngine::new())?;

        let p: std::path::PathBuf = tempfile::tempdir()?.into_path().join("sqldb-log");
        scan_isolation(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    fn set(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"key1".to_vec(), b"val1".to_vec())?;
        tx.set(b"key2".to_vec(), b"val2".to_vec())?;
        tx.set(b"key2".to_vec(), b"val3".to_vec())?;
        tx.set(b"key3".to_vec(), b"val4".to_vec())?;
        tx.set(b"key4".to_vec(), b"val5".to_vec())?;
        tx.commit()?;

        let tx1 = mvcc.begin()?;
        let tx2 = mvcc.begin()?;
        tx1.set(b"key1".to_vec(), b"val1-1".to_vec())?;
        tx1.set(b"key2".to_vec(), b"val3-1".to_vec())?;
        tx1.set(b"key2".to_vec(), b"val3-2".to_vec())?;

        tx2.set(b"key3".to_vec(), b"val4-1".to_vec())?;
        tx2.set(b"key4".to_vec(), b"val5-1".to_vec())?;

        tx1.commit()?;
        tx2.commit()?;

        let tx = mvcc.begin()?;
        assert_eq!(tx.get(b"key1".to_vec())?, Some(b"val1-1".to_vec()));
        assert_eq!(tx.get(b"key2".to_vec())?, Some(b"val3-2".to_vec()));
        assert_eq!(tx.get(b"key3".to_vec())?, Some(b"val4-1".to_vec()));
        assert_eq!(tx.get(b"key4".to_vec())?, Some(b"val5-1".to_vec()));
        Ok(())
    }

    #[test]
    fn test_set() -> Result<()> {
        set(MemoryEngine::new())?;
        let p: std::path::PathBuf = tempfile::tempdir()?.into_path().join("sqldb-log");
        set(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    fn set_conflict(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"key1".to_vec(), b"val1".to_vec())?;
        tx.set(b"key2".to_vec(), b"val2".to_vec())?;
        tx.set(b"key2".to_vec(), b"val3".to_vec())?;
        tx.set(b"key3".to_vec(), b"val4".to_vec())?;
        tx.set(b"key4".to_vec(), b"val5".to_vec())?;
        tx.commit()?;

        let tx1 = mvcc.begin()?;
        let tx2 = mvcc.begin()?;

        tx1.set(b"key1".to_vec(), b"val1-1".to_vec())?;
        tx1.set(b"key1".to_vec(), b"val1-2".to_vec())?;

        assert_eq!(
            tx2.set(b"key1".to_vec(), b"val1-3".to_vec()),
            Err(Error::WriteConflict)
        );

        let tx3 = mvcc.begin()?;
        tx3.set(b"key5".to_vec(), b"val6".to_vec())?;
        tx3.commit()?;

        assert_eq!(
            tx1.set(b"key5".to_vec(), b"val6-1".to_vec()),
            Err(Error::WriteConflict)
        );

        tx1.commit()?;
        Ok(())
    }

    #[test]
    fn test_set_conflict() -> Result<()> {
        set_conflict(MemoryEngine::new())?;
        let p: std::path::PathBuf = tempfile::tempdir()?.into_path().join("sqldb-log");
        set_conflict(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    fn delete(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"key1".to_vec(), b"val1".to_vec())?;
        tx.set(b"key2".to_vec(), b"val2".to_vec())?;
        tx.set(b"key3".to_vec(), b"val3".to_vec())?;
        tx.delete(b"key2".to_vec())?;
        tx.delete(b"key3".to_vec())?;
        tx.set(b"key3".to_vec(), b"val3-1".to_vec())?;
        tx.commit()?;

        let tx1 = mvcc.begin()?;
        assert_eq!(tx1.get(b"key2".to_vec())?, None);

        let iter = tx1.scan_prefix(b"ke".to_vec())?;
        assert_eq!(
            iter,
            vec![
                ScanResult {
                    key: b"key1".to_vec(),
                    value: b"val1".to_vec()
                },
                ScanResult {
                    key: b"key3".to_vec(),
                    value: b"val3-1".to_vec()
                }
            ]
        );

        Ok(())
    }

    #[test]
    fn test_delete() -> Result<()>{
        delete(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        delete(DiskEngine::new(p.clone())?)?;
        remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    fn delete_conflict(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"key1".to_vec(), b"val1".to_vec())?;
        tx.set(b"key2".to_vec(), b"val2".to_vec())?;
        tx.commit()?;

        let tx1 = mvcc.begin()?;
        let tx2 = mvcc.begin()?;
        tx1.delete(b"key1".to_vec())?;
        tx1.set(b"key2".to_vec(), b"val2-1".to_vec())?;

        assert_eq!(
            tx2.delete(b"key1".to_vec()),
            Err(Error::WriteConflict)
        );

        assert_eq!(
            tx2.delete(b"key2".to_vec()),
            Err(Error::WriteConflict)
        );

        Ok(())
    }

    #[test]
    fn test_delete_conflict() -> Result<()>{
        delete_conflict(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        delete_conflict(DiskEngine::new(p.clone())?)?;
        remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }
    

    fn dirty_read(eng:impl Engine) -> Result<()>{
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"key1".to_vec(), b"val1".to_vec())?;
        tx.set(b"key2".to_vec(), b"val2".to_vec())?;
        tx.set(b"key3".to_vec(), b"val3".to_vec())?;
        tx.commit()?;

        let tx1 = mvcc.begin()?;
        let tx2 = mvcc.begin()?;

        tx2.set(b"key1".to_vec(), b"val1-1".to_vec())?;

        assert_eq!(tx1.get(b"key1".to_vec())?,Some(b"val1".to_vec()));

        Ok(())
    }

    #[test]
    fn test_dirty_read() -> Result<()>{
        dirty_read(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        dirty_read(DiskEngine::new(p.clone())?)?;
        remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }   

    fn unrepeatable_read(eng:impl Engine) -> Result<()>{
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"key1".to_vec(), b"val1".to_vec())?;
        tx.set(b"key2".to_vec(), b"val2".to_vec())?;
        tx.set(b"key3".to_vec(), b"val3".to_vec())?;
        tx.commit()?;

        let tx1 = mvcc.begin()?;
        let tx2 = mvcc.begin()?;

        tx2.set(b"key1".to_vec(), b"val1-1".to_vec())?;
        assert_eq!(tx1.get(b"key1".to_vec())?,Some(b"val1".to_vec()));
        tx2.commit()?;
        assert_eq!(tx1.get(b"key1".to_vec())?,Some(b"val1".to_vec()));
        Ok(())
    }

    #[test]
    fn test_unrepeatable_read() -> Result<()>{
        unrepeatable_read(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        unrepeatable_read(DiskEngine::new(p.clone())?)?;
        remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }   

    fn phantom_read(eng:impl Engine) -> Result<()>{
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"key1".to_vec(), b"val1".to_vec())?;
        tx.set(b"key2".to_vec(), b"val2".to_vec())?;
        tx.set(b"key3".to_vec(), b"val3".to_vec())?;
        tx.commit()?;

        let tx1 = mvcc.begin()?;
        let tx2 = mvcc.begin()?;

        let iter1 = tx1.scan_prefix(b"key".to_vec())?;
        assert_eq!(
            iter1,
            vec![
                ScanResult{
                    key: b"key1".to_vec(),
                    value: b"val1".to_vec()
                },
                ScanResult{
                    key: b"key2".to_vec(),
                    value: b"val2".to_vec()
                },
                ScanResult{
                    key: b"key3".to_vec(),
                    value: b"val3".to_vec()
                }
            ]
        );

        tx2.set(b"key2".to_vec(), b"val2-1".to_vec())?;
        tx2.set(b"key4".to_vec(), b"val4".to_vec())?;
        tx2.commit()?;

        let iter1 = tx1.scan_prefix(b"key".to_vec())?;
        assert_eq!(
            iter1,
            vec![
                ScanResult{
                    key: b"key1".to_vec(),
                    value: b"val1".to_vec()
                },
                ScanResult{
                    key: b"key2".to_vec(),
                    value: b"val2".to_vec()
                },
                ScanResult{
                    key: b"key3".to_vec(),
                    value: b"val3".to_vec()
                }
            ]
        );

        Ok(())
    }

    #[test]
    fn test_phantom_read() -> Result<()>{
        phantom_read(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        phantom_read(DiskEngine::new(p.clone())?)?;
        remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    fn rollback(eng:impl Engine) -> Result<()>{
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"key1".to_vec(), b"val1".to_vec())?;
        tx.set(b"key2".to_vec(), b"val2".to_vec())?;
        tx.set(b"key3".to_vec(), b"val3".to_vec())?;
        tx.commit()?;


        let tx1 = mvcc.begin()?;
        tx1.set(b"key1".to_vec(), b"val1-1".to_vec())?;
        tx1.set(b"key2".to_vec(), b"val2-2".to_vec())?;
        tx1.set(b"key3".to_vec(), b"val3-3".to_vec())?;
        tx1.rollback()?;

        let tx2 = mvcc.begin()?;
        assert_eq!(tx2.get(b"key1".to_vec())?,Some(b"val1".to_vec()));
        assert_eq!(tx2.get(b"key2".to_vec())?,Some(b"val2".to_vec()));
        assert_eq!(tx2.get(b"key3".to_vec())?,Some(b"val3".to_vec()));

        Ok(())
    }

    #[test]
    fn test_rollback() -> Result<()>{
        rollback(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        rollback(DiskEngine::new(p.clone())?)?;
        remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }  





}
