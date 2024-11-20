use crate::sql::parser::ast::{self, Consts, Expression};

#[derive(Debug,PartialEq)]
pub enum DataType{
    Integer,
    String,
    Float,
    Boolean
}

#[derive(Debug,PartialEq)]
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
}