use std::iter::Peekable;

use lexer::{Keyword, Lexer, Token};

use crate::error::{Error, Result};

pub mod lexer;
pub mod ast;

/**
 * 解析器
 */
struct Parser<'a> {
    lexer: Peekable<Lexer<'a>>,
}

impl<'a> Parser<'a> {
    pub fn new(input: &'a str) -> Self {
        Parser {
            lexer: Lexer::new(&input).peekable(),
        }
    }

    /**
     * 解析, 获取抽象语法树
     */
    pub fn parse(&mut self) -> Result<ast::Statement>{
        let stmt = self.parse_statement()?;
        Ok(stmt)
    }

    fn parse_statement(&mut self) -> Result<ast::Statement>{
        match self.peek()? {
            Some(Token::Keyword(Keyword::Create)) => self.parse_ddl(),
            Some(t) => todo!(),
            None => Err(Error::Parse(format!("[Parser] unexpected end of input")))
        }
    }

    fn parse_ddl(&mut self) -> Result<ast::Statement> {
        match self.next()? {
            Token::Keyword(Keyword::Create) => match self.next()? {
                Token::Keyword(Keyword::Table) => todo!(),
                token => Err(Error::Parse(format!("[Parser] unexpected end of input"))),
            },
            token =>  Err(Error::Parse(format!("[Parser] unexpected end of input"))),
        }
    }


    fn peek(&mut self) -> Result<Option<Token>>{
        self.lexer.peek().cloned().transpose()
    }

    fn next(&mut self) -> Result<Token>{
        self.lexer.next().unwrap_or_else(|| Err(Error::Parse(format!("[Parser] unexpected end of input"))))
    }
}


