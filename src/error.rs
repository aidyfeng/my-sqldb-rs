use std::{
    fmt::{self, Display},
    num::{ParseFloatError, ParseIntError},
    sync::PoisonError,
};

use bincode::ErrorKind;
use serde::{de, ser};

//自定义result 类型
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    Parse(String),
    Internal(String),
    WriteConflict,
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

impl std::error::Error for Error {}

impl ser::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::Internal(msg.to_string())
    }
}

impl de::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::Internal(msg.to_string())
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Parse(err) => write!(f, "parse error {}", err),
            Error::Internal(err) => write!(f, "internal error {}", err),
            Error::WriteConflict => write!(f, "write conflict,try transaction"),
        }
    }
}
