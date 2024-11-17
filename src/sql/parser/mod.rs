use std::iter::Peekable;

use ast::Column;
use lexer::{Keyword, Lexer, Token};

use crate::{error::{Error, Result}, types::DataType};

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
            Some(t) => Err(Error::Parse(format!("[Parser] unexpected token {}",t))),
            None => Err(Error::Parse(format!("[Parser] unexpected end of input")))
        }
    }

    /**
     * 解析ddl类型
     */
    fn parse_ddl(&mut self) -> Result<ast::Statement> {
        match self.next()? {
            Token::Keyword(Keyword::Create) => match self.next()? {
                Token::Keyword(Keyword::Table) => self.parse_ddl_create_table(),
                token => Err(Error::Parse(format!("[Parser] unexpected token {}",token))),
            },
            token =>  Err(Error::Parse(format!("[Parser] unexpected token {}",token))),
        }
    }

    /**
     * 解析create table
     */
    fn parse_ddl_create_table(&mut self) -> Result<ast::Statement>{
        //标名
        let table_name = self.next_ident()?;
        //表名之后期望是括号
        self.next_expected(Token::OpenParen)?;

        //解析列信息
        let mut colunm = Vec::<Column>::new();
        loop {
            
        }

    }

    fn parse_ddl_column(&mut self) -> Result<ast::Column>{
        let mut column = Column { 
            name: self.next_ident()?, 
            datatype: match self.next()? {
               Token::Keyword(Keyword::Bool) | Token::Keyword(Keyword::Boolean) =>  DataType::Boolean,
               Token::Keyword(Keyword::Double) | Token::Keyword(Keyword::Float) => DataType::Float,
               Token::Keyword(Keyword::String) |Token::Keyword(Keyword::Text) | Token::Keyword(Keyword::Varchar) => DataType::String,
               Token::Keyword(Keyword::Integer) | Token::Keyword(Keyword::Int) => DataType::Integer,
               token => return Err(Error::Parse(format!("[Parser] Expected token {}",token)))
            }, 
            nullable: None, 
            default: None 
        };

        //解析列的默认值, 以及是否可以为空
        while let Some(Token::Keyword(keyword)) = self.next_if_keywork(){
            match keyword{
                Keyword::Null => column.nullable = Some(true),
                Keyword::Not =>{
                    self.next_expected(Token::Keyword(Keyword::Null))?;
                    column.nullable = Some(false)
                },
                Keyword::Default => todo!(),
                k => return Err(Error::Parse(format!("[Parser] Unexpected keyword {}",k)))
            }
        }

        Ok(column)
    }

    fn next_ident(&mut self) -> Result<String>{
        match self.next()? {
            Token::Ident(ident) => Ok(ident),
            token => Err(Error::Parse(format!("[Parser] Expected ident, got token {}",token)))

        }
    }

    /**
     * 判断下一个值是否期待值
     */
    fn next_expected(&mut self,expected :Token) -> Result<()> {
        let token = self.next()?;
        if token != expected{
            return Err(Error::Parse(format!("[Parser] Expected token {}, got {}",expected,token)))
        }
        Ok(())
    }


    fn peek(&mut self) -> Result<Option<Token>>{
        self.lexer.peek().cloned().transpose()
    }

    fn next(&mut self) -> Result<Token>{
        self.lexer.next().unwrap_or_else(|| Err(Error::Parse(format!("[Parser] unexpected end of input"))))
    }

    fn next_if<F:Fn(&Token) -> bool>(&mut self,predicate:F) -> Option<Token>{
        self.peek().unwrap_or(None).filter(predicate)?;
        self.next().ok()
    }

    fn next_if_keywork(&mut self) -> Option<Token>{
        self.next_if(|it| matches!(it,Token::Keyword(_)))
    }
}


