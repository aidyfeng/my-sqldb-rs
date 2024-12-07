use serde::{Deserialize, Serialize};

use crate::{error::Result, sql::parser::ast::{ Consts, Expression}};

#[derive(Debug,PartialEq,Serialize,Deserialize)]
pub enum DataType{
    Integer,
    String,
    Float,
    Boolean
}

#[derive(Debug,PartialEq,Serialize,Deserialize,Clone)]
pub enum Value{
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String)
}

impl Value{
    pub fn from_expression(expr : Expression) -> Self{
        match expr {
            Expression::Consts(Consts::Null) => Self::Null,
            Expression::Consts(Consts::Boolean(bool)) => Self::Boolean(bool),
            Expression::Consts(Consts::Float(f)) => Self::Float(f),
            Expression::Consts(Consts::Integer(i)) => Self::Integer(i),
            Expression::Consts(Consts::String(s)) => Self::String(s),
        }
    }

    pub fn datatype(&self) -> Option<DataType>{
        match self {
            Self::Null => None,
            Self::Boolean(_) => Some(DataType::Boolean),
            Self::Float(_) => Some(DataType::Float),
            Self::Integer(_) => Some(DataType::Integer),
            Self::String(_) => Some(DataType::String)
        }
    }
}

pub type Row = Vec<Value>;