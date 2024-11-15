use std::{iter::Peekable, str::Chars, string};

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

pub struct Lexer<'a> {
    iter: Peekable<Chars<'a>>,
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
     * 扫描拿到第一个token
     */
    fn scan(&mut self) -> Result<Option<Token>> {
        //消除字符串中的空白字符
        self.erase_whitespace();
        match self.iter.peek() {
            Some('\'') => self.scan_string(), //扫描字符串
            _ => todo!(),
            None => Ok(None),
        };
        todo!()
    }

    fn scan_string(&mut self) -> Result<Option<Token>> {
        if self.next_if(|it| it == '\'').is_none() {
            return Ok(None);
        }

        let mut value = String::new();
        loop {
            match self.iter.next() {
                Some('\'') => break,
                Some(c) => value.push(c),
                None => return Err(Error::Parse(format!("[Lexer] unexpected end of string")))
            }
        }

        Ok(Some(Token::String(value)))
    }
}
