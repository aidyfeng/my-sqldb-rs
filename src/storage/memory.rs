use std::collections::{btree_map, BTreeMap};

use crate::error::Result;

pub struct MemoryEngine {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl MemoryEngine {
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
        }
    }
}

impl super::engine::Engine for MemoryEngine {
    type EngineIterator ;

    fn set(&mut self,key: Vec<u8>,value: Vec<u8>) -> Result<()> {
        todo!()
    }

    fn get(&mut self,key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        todo!()
    }

    fn delete(&mut self,key : Vec<u8>) -> Result<()> {
        todo!()
    }

    fn scan(&mut self,range: impl std::ops::RangeBounds<Vec<u8>>) -> Self::EngineIterator {
        todo!()
    }
    
    fn scan_prefix(&mut self,prefix:Vec<u8>) -> Self::EngineIterator{
        std::todo!()
    }
}

pub struct MemoryEnginIterator<'a>{
    inner : btree_map::Range<'a,Vec<u8>,Vec<u8>>
}

impl<'a>  MemoryEnginIterator<'a>{
    
}

impl<'a> Iterator for MemoryEnginIterator<'a> {
    type Item = Result<(Vec<u8>,Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl<'a> DoubleEndedIterator for MemoryEnginIterator<'a>{
    fn next_back(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl<'a> super::engine::EngineIterator for MemoryEnginIterator<'a>{
    
}
