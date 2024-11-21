use std::{fmt::Display, iter::Peekable, str::Chars};

use crate::error::{Error, Result};

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // 关键字
    Keyword(Keyword),
    // 其他类型的字符串Token，比如表名、列名
    Ident(String),
    // 字符串类型的数据
    String(String),
    // 数值类型，比如整数和浮点数
    Number(String),
    // 左括号 (
    OpenParen,
    // 右括号 )
    CloseParen,
    // 逗号 ,
    Comma,
    // 分号 ;
    Semicolon,
    // 星号 *
    Asterisk,
    // 加号 +
    Plus,
    // 减号 -
    Minus,
    // 斜杠 /
    Slash,
}

impl Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Token::Keyword(keyword) => keyword.to_str(),
            Token::Ident(ident) => ident,
            Token::String(v) => v,
            Token::Number(n) => n,
            Token::OpenParen => "(",
            Token::CloseParen => ")",
            Token::Comma => ",",
            Token::Semicolon => ";",
            Token::Asterisk => "*",
            Token::Plus => "+",
            Token::Minus => "-",
            Token::Slash => "/",
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Keyword {
    Create,
    Table,
    Int,
    Integer,
    Boolean,
    Bool,
    String,
    Text,
    Varchar,
    Float,
    Double,
    Select,
    From,
    Insert,
    Into,
    Values,
    True,
    False,
    Default,
    Not,
    Null,
    Primary,
    Key,
}

impl Keyword {
    pub fn from_str(ident: &str) -> Option<Self> {
        Some(match ident.to_uppercase().as_ref() {
            "CREATE" => Keyword::Create,
            "TABLE" => Keyword::Table,
            "INT" => Keyword::Int,
            "INTEGER" => Keyword::Integer,
            "BOOLEAN" => Keyword::Boolean,
            "BOOL" => Keyword::Bool,
            "STRING" => Keyword::String,
            "TEXT" => Keyword::Text,
            "VARCHAR" => Keyword::Varchar,
            "FLOAT" => Keyword::Float,
            "DOUBLE" => Keyword::Double,
            "SELECT" => Keyword::Select,
            "FROM" => Keyword::From,
            "INSERT" => Keyword::Insert,
            "INTO" => Keyword::Into,
            "VALUES" => Keyword::Values,
            "TRUE" => Keyword::True,
            "FALSE" => Keyword::False,
            "DEFAULT" => Keyword::Default,
            "NOT" => Keyword::Not,
            "NULL" => Keyword::Null,
            "PRIMARY" => Keyword::Primary,
            "KEY" => Keyword::Key,
            _ => return None,
        })
    }

    pub fn to_str(&self) -> &str {
        match self {
            Keyword::Create => "CREATE",
            Keyword::Table => "TABLE",
            Keyword::Int => "INT",
            Keyword::Integer => "INTEGER",
            Keyword::Boolean => "BOOLEAN",
            Keyword::Bool => "BOOL",
            Keyword::String => "STRING",
            Keyword::Text => "TEXT",
            Keyword::Varchar => "VARCHAR",
            Keyword::Float => "FLOAT",
            Keyword::Double => "DOUBLE",
            Keyword::Select => "SELECT",
            Keyword::From => "FROM",
            Keyword::Insert => "INSERT",
            Keyword::Into => "INTO",
            Keyword::Values => "VALUES",
            Keyword::True => "TRUE",
            Keyword::False => "FALSE",
            Keyword::Default => "DEFAULT",
            Keyword::Not => "NOT",
            Keyword::Null => "NULL",
            Keyword::Primary => "PRIMARY",
            Keyword::Key => "KEY",
        }
    }
}

impl Display for Keyword {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.to_str())
    }
}

pub struct Lexer<'a> {
    iter: Peekable<Chars<'a>>,
}

/**
 * 自定义迭代器
 */
impl<'a> Iterator for Lexer<'a> {
    type Item = Result<Token>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.scan() {
            Ok(Some(token)) => Some(Ok(token)),
            Ok(None) => self
                .iter
                .peek()
                .map(|it| Err(Error::Parse(format!("[Lexer] unexpected character {}", it)))),
            Err(err) => Some(Err(err)),
        }
    }
}

impl<'a> Lexer<'a> {
    pub fn new(sql_test: &'a str) -> Self {
        Self {
            iter: sql_test.chars().peekable(),
        }
    }

    /**
     * 消除空白字符串
     */
    fn erase_whitespace(&mut self) {
        self.next_while(|it| it.is_whitespace());
    }

    /**
     * 如果满足条件,则跳转下一个
     */
    fn next_if<F: Fn(char) -> bool>(&mut self, predicate: F) -> Option<char> {
        self.iter.peek().filter(|&&it| predicate(it))?;
        self.iter.next()
    }

    /**
     * 判断当前字符是否满足条件,如果是的话跳转到下一个
     */
    fn next_while<F: Fn(char) -> bool>(&mut self, predicate: F) -> Option<String> {
        let mut value = String::new();
        while let Some(c) = self.next_if(&predicate) {
            value.push(c);
        }
        Some(value).filter(|it| !it.is_empty())
    }

    /**
     * 只有token类型,才能跳转到下一个转移
     */
    fn next_if_token<F: Fn(char) -> Option<Token>>(&mut self, predicate: F) -> Option<Token> {
        let token = self.iter.peek().and_then(|&it| predicate(it))?;
        self.iter.next();
        Some(token)
    }

    /**
     * 扫描拿到第一个token
     */
    fn scan(&mut self) -> Result<Option<Token>> {
        //消除字符串中的空白字符
        self.erase_whitespace();
        match self.iter.peek() {
            Some('\'') => self.scan_string(),                     //扫描字符串
            Some(c) if c.is_ascii_digit() => Ok(self.scan_num()), // 扫描数字
            Some(c) if c.is_ascii_alphabetic() => Ok(self.scan_ident()), // 扫描字符
            Some(_) => Ok(self.scan_symbol()),                    // 扫描符号
            None => Ok(None),
        }
    }

    /**
     * 扫描字符串
     */
    fn scan_string(&mut self) -> Result<Option<Token>> {
        if self.next_if(|it| it == '\'').is_none() {
            return Ok(None);
        }

        let mut value = String::new();
        loop {
            match self.iter.next() {
                Some('\'') => break,
                Some(c) => value.push(c),
                None => return Err(Error::Parse(format!("[Lexer] unexpected end of string"))),
            }
        }

        Ok(Some(Token::String(value)))
    }

    /**
     * 扫描数字
     */
    fn scan_num(&mut self) -> Option<Token> {
        //获取数字
        let mut num = self.next_while(|it| it.is_ascii_digit())?;

        //判断是否有小数点, 如果有小数点, 则是浮点数, 继续扫描
        if let Some(sep) = self.next_if(|it| it == '.') {
            num.push(sep);
            while let Some(c) = self.next_if(|it| it.is_ascii_digit()) {
                num.push(c);
            }
        }

        Some(Token::Number(num))
    }

    /**
     * 扫描Ident字符, 例如表名,列名, 也可能是关键字
     */
    fn scan_ident(&mut self) -> Option<Token> {
        let mut value = self.next_if(|it| it.is_alphanumeric())?.to_string();

        while let Some(c) = self.next_if(|it| it.is_alphanumeric() || it == '_') {
            value.push(c);
        }

        Some(Keyword::from_str(&value).map_or_else(|| Token::Ident(value), Token::Keyword))
    }

    fn scan_symbol(&mut self) -> Option<Token> {
        self.next_if_token(|it| {
            Some(match it {
                '*' => Token::Asterisk,
                '(' => Token::OpenParen,
                ')' => Token::CloseParen,
                ',' => Token::Comma,
                ';' => Token::Semicolon,
                '+' => Token::Plus,
                '-' => Token::Minus,
                '/' => Token::Slash,
                _ => return None,
            })
        })
    }
}

#[cfg(test)]
mod test {
    
    use crate::{
        error::Result,
        sql::parser::lexer::{Keyword, Token},
    };

    use super::Lexer;

    #[test]
    fn test_lexer_create_table() -> Result<()> {
        let tokens = Lexer::new(
            "
                create table tbl(
                    id1 int primary key,
                    id2 integer
                );
                ",
        )
        .peekable()
        .collect::<Result<Vec<_>>>()?;

        // println!("{:?}",tokens);

        assert_eq!(
            tokens,
            vec![
                Token::Keyword(Keyword::Create),
                Token::Keyword(Keyword::Table),
                Token::Ident("tbl".to_string()),
                Token::OpenParen,
                Token::Ident("id1".to_string()),
                Token::Keyword(Keyword::Int),
                Token::Keyword(Keyword::Primary),
                Token::Keyword(Keyword::Key),
                Token::Comma,
                Token::Ident("id2".to_string()),
                Token::Keyword(Keyword::Integer),
                Token::CloseParen,
                Token::Semicolon
            ]
        );

        let tokens2 = Lexer::new(
            "CREATE table tbl
                        (
                            id1 int primary key,
                            id2 integer,
                            c1 bool null,
                            c2 boolean not null,
                            c3 float null,
                            c4 double,
                            c5 string,
                            c6 text,
                            c7 varchar default 'foo',
                            c8 int default 100,
                            c9 integer
                        );
                        ",
        )
        .peekable()
        .collect::<Result<Vec<_>>>()?;

        println!("{:?}", tokens2);

        assert!(tokens2.len() > 0);

        Ok(())
    }

    #[test]
    fn test_lexer_insert_into() -> Result<()> {
        let tokens1 = Lexer::new(
            "
                    insert into tbl values(1,2,'3',true, false,4.55);
                ",
        )
        .peekable()
        .collect::<Result<Vec<_>>>()?;

        // println!("{:?}",tokens1);

        assert_eq!(
            tokens1,
            vec![
                Token::Keyword(Keyword::Insert),
                Token::Keyword(Keyword::Into),
                Token::Ident("tbl".to_string()),
                Token::Keyword(Keyword::Values),
                Token::OpenParen,
                Token::Number("1".to_string()),
                Token::Comma,
                Token::Number("2".to_string()),
                Token::Comma,
                Token::String("3".to_string()),
                Token::Comma,
                Token::Keyword(Keyword::True),
                Token::Comma,
                Token::Keyword(Keyword::False),
                Token::Comma,
                Token::Number("4.55".to_string()),
                Token::CloseParen,
                Token::Semicolon,
            ]
        );

        let tokens2 = Lexer::new("INSERT INTO       tbl (id, name, age) values (100, 'db', 10);")
            .peekable()
            .collect::<Result<Vec<_>>>()?;

        assert_eq!(
            tokens2,
            vec![
                Token::Keyword(Keyword::Insert),
                Token::Keyword(Keyword::Into),
                Token::Ident("tbl".to_string()),
                Token::OpenParen,
                Token::Ident("id".to_string()),
                Token::Comma,
                Token::Ident("name".to_string()),
                Token::Comma,
                Token::Ident("age".to_string()),
                Token::CloseParen,
                Token::Keyword(Keyword::Values),
                Token::OpenParen,
                Token::Number("100".to_string()),
                Token::Comma,
                Token::String("db".to_string()),
                Token::Comma,
                Token::Number("10".to_string()),
                Token::CloseParen,
                Token::Semicolon,
            ]
        );

        Ok(())
    }

    #[test]
    fn test_lexer_select() -> Result<()> {
        let tokens = Lexer::new("select * from tbl;")
            .peekable()
            .collect::<Result<Vec<_>>>()?;
        // println!("{:?}",tokens);

        assert_eq!(
            tokens,
            vec![
                Token::Keyword(Keyword::Select),
                Token::Asterisk,
                Token::Keyword(Keyword::From),
                Token::Ident("tbl".to_string()),
                Token::Semicolon
            ]
        );
        Ok(())
    }
}
