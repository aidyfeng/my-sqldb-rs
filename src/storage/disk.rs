use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufWriter, Seek, Write},
};

use crate::error::Result;

use super::engine::{Engine, EngineIterator};

/**
 * 定义磁盘存储引擎
 * 使用bitcast存储模型
**/

pub type KeyDir = BTreeMap<Vec<u8>, (u64, u32)>;

const LOG_HEADER_SIZE: u32 = 8;

pub struct DistEngine {
    keydir: KeyDir,
    log: Log,
}

impl Engine for DistEngine {
    type EngineIterator<'a> = DiskEngineIterator;

    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        //先写日志
        let (offset, size) = self.log.write_entry(&key, Some(&value))?;
        //更新内存索引
        //100--------|----150
        //           130
        //val_size = 20
        let val_size = value.len() as u32;
        self.keydir
            .insert(key, (offset + size as u64 - val_size as u64, val_size));
        Ok(())
    }

    fn get(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        match self.keydir.get(&key) {
            Some((offset, size)) => {
                let val = self.log.read_value(*offset, *size)?;
                Ok(Some(val))
            }
            None => Ok(None),
        }
    }

    fn delete(&mut self, key: Vec<u8>) -> Result<()> {
        //写日志
        self.log.write_entry(&key, None)?;
        //删除内存数据
        self.keydir.remove(&key);
        Ok(())
    }

    fn scan(&mut self, range: impl std::ops::RangeBounds<Vec<u8>>) -> Self::EngineIterator<'_> {
        todo!()
    }
}

struct Log {
    file: File,
}

impl Log {
    fn write_entry(&mut self, key: &Vec<u8>, value: Option<&Vec<u8>>) -> Result<(u64, u32)> {
        //将文件偏移量移动到文件末尾
        let offset = self.file.seek(std::io::SeekFrom::End(0))?;
        let key_size = key.len();
        let val_size = value.map_or(0, |it| it.len());
        let total_size = key_size + val_size + LOG_HEADER_SIZE as usize;
        //数据写入磁盘
        //写入 key_size,val_size,key,value
        let mut writer = BufWriter::with_capacity(total_size, &self.file);
        writer.write_all(&key_size.to_be_bytes())?;
        writer.write_all(&value.map_or(-1, |it| it.len() as i32).to_be_bytes())?;
        writer.write_all(&key)?;
        if let Some(v) = value {
            writer.write_all(v)?;
        }
        writer.flush()?;
        Ok((offset, total_size as u32))
    }

    fn read_value(&mut self, offset: u64, size: u32) -> Result<Vec<u8>> {
        todo!()
    }
}

pub struct DiskEngineIterator {}

impl Iterator for DiskEngineIterator {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl DoubleEndedIterator for DiskEngineIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl EngineIterator for DiskEngineIterator {}
