use crate::types::{DataType, Value};


#[derive(Debug)]
pub struct Table{
    pub name:String,
    pub columns:Vec<Column>,
}

#[derive(Debug)]
pub struct Column{
    pub name:String,
    pub datatype:DataType,
    pub nullable:bool,
    pub default:Option<Value>
}   

