//自定义result 类型
pub type Result<T> = std::result::Result<T,Error>;

#[derive(Debug,Clone, PartialEq)]
pub enum Error{
    Parse(String)
}