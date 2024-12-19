use std::{num::{ParseFloatError, ParseIntError}, sync::PoisonError};

use bincode::ErrorKind;

//自定义result 类型
pub type Result<T> = std::result::Result<T,Error>;

#[derive(Debug,Clone, PartialEq)]
pub enum Error{
    Parse(String),
    Internal(String),
    WriteConflict
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

impl<T> From<PoisonError<T>> for Error {
    fn from(value: PoisonError<T>) -> Self {
        Error::Internal(value.to_string())
    }
}

impl From<Box<ErrorKind>> for Error {
    fn from(value: Box<ErrorKind>) -> Self {
        Error::Internal(value.to_string())
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Error::Internal(value.to_string())
    }
}