use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    intrinsics::logf64,
    io::{BufReader, BufWriter, Read, Seek, Write},
    path::PathBuf,
};

use fs4::fs_std::FileExt;
use serde::de::value;

use crate::error::Result;

use super::engine::{Engine, EngineIterator};

/**
 * 定义磁盘存储引擎
 * 使用bitcast存储模型
**/

pub type KeyDir = BTreeMap<Vec<u8>, (u64, u32)>;

const LOG_HEADER_SIZE: u32 = 8;

pub struct DiskEngine {
    keydir: KeyDir,
    log: Log,
}

impl DiskEngine {
    fn new(file_path: PathBuf) -> Result<Self> {
        let log = Log::new(file_path)?;
        //从log恢复keydir

        todo!()
    }
}

impl Engine for DiskEngine {
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
    fn new(file_path: PathBuf) -> Result<Self> {
        //如果目录不存在则创建
        if let Some(dir) = file_path.parent() {
            if !dir.exists() {
                fs::create_dir_all(&dir)?;
            }
        }

        //打开文件
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&file_path)?;

        //加文件锁,保证只能同时只能有一个服务使用
        file.try_lock_exclusive()?;

        Ok(Self { file })
    }

    fn build_keydir(&mut self) -> Result<KeyDir> {
        let mut keydir = KeyDir::new();
        let file_len = self.file.metadata()?.len();
        let mut buf_reader = BufReader::new(&self.file);

        let mut offset = 0;
        loop {
            if offset >= file_len {
                break;
            }

            let (key, value_size) = Self::read_entry(&mut buf_reader, offset)?;

            let key_size = key.len() as u64;
            if value_size == -1 {
                keydir.remove(&key);
                offset += LOG_HEADER_SIZE as u64 + key_size;
            } else {
                keydir.insert(
                    key,
                    (
                        offset + LOG_HEADER_SIZE as u64 + key_size,
                        value_size as u32,
                    ),
                );
                offset += LOG_HEADER_SIZE as u64 + key_size + value_size as u64;
            }
        }

        Ok(keydir)
    }

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
        //跳转到偏移量位置
        self.file.seek(std::io::SeekFrom::Start(offset))?;
        let mut buffer = vec![0; size as usize];
        //读取数据到buffer
        self.file.read_exact(&mut buffer)?;
        Ok(buffer)
    }

    fn read_entry(buf_reader: &mut BufReader<&File>, offset: u64) -> Result<(Vec<u8>, i32)> {
        buf_reader.seek(std::io::SeekFrom::Start(offset));
        let mut len_buf = [0; 4];

        //读取key_size
        buf_reader.read_exact(&mut len_buf);
        let key_size = u32::from_be_bytes(len_buf);

        //读取value_size
        buf_reader.read_exact(&mut len_buf);
        let value_size = i32::from_be_bytes(len_buf);

        //读取key
        let mut key = vec![0; key_size as usize];
        buf_reader.read_exact(&mut key);

        Ok((key, value_size))
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
