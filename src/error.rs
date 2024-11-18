use std::num::{ParseFloatError, ParseIntError};

//自定义result 类型
pub type Result<T> = std::result::Result<T,Error>;

#[derive(Debug,Clone, PartialEq)]
pub enum Error{
    Parse(String)
}

impl From<ParseFloatError> for Error {
    fn from(value: ParseFloatError) -> Self {
        Error::Parse(value.to_string())
    }
}

impl From<ParseIntError> for Error {
    fn from(value: ParseIntError) -> Self {
        Error::Parse(value.to_string())
    }
}