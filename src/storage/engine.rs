use std::ops::RangeBounds;

use crate::error::Result;

/**
 * 抽象存储引擎定义
 */
pub trait Engine {

    type EngineIterator : EngineIterator;
    /**
     * 设置 key,value
     */
    fn set(&mut self,key: Vec<u8>,value: Vec<u8>) -> Result<()>;
    
    /**
     * 获取key对应的数值
     */
    fn get(&mut self,key: Vec<u8>) -> Result<Option<Vec<u8>>>;

    /**
     * 删除key对应的数值, 如果key不存在, 则忽略
     */
    fn delete(&mut self,key : Vec<u8>) -> Result<()>;

    /**
     * 扫描
     */
    fn scan(&mut self,range: impl RangeBounds<Vec<u8>>) -> Self::EngineIterator;

    /**
     * 前缀扫描
     */
    fn scan_prefix(&mut self,prefix:Vec<u8>) -> Self::EngineIterator{
        todo!()
    }
}

pub trait EngineIterator:DoubleEndedIterator<Item = Result<(Vec<u8>,Vec<u8>)>> {
    
}