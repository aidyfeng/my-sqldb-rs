use std::iter::Peekable;

use ast::Column;
use lexer::{Keyword, Lexer, Token};

use crate::{
    error::{Error, Result},
    types::DataType,
};

pub mod ast;
pub mod lexer;

/**
 * 解析器
 */
pub struct Parser<'a> {
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
    pub fn parse(&mut self) -> Result<ast::Statement> {
        let stmt = self.parse_statement()?;
        //期望sql结束后跟的是分号
        self.next_expected(Token::Semicolon)?;
        //分号后不能跟其他符号
        if let Some(token) = self.peek()? {
            return Err(Error::Parse(format!("[Parser] Unexpected token {}", token)));
        }
        Ok(stmt)
    }

    fn parse_statement(&mut self) -> Result<ast::Statement> {
        match self.peek()? {
            Some(Token::Keyword(Keyword::Create)) => self.parse_ddl(),
            Some(Token::Keyword(Keyword::Select)) => self.parse_select(),
            Some(Token::Keyword(Keyword::Insert)) => self.parse_insert(),
            Some(t) => Err(Error::Parse(format!("[Parser] unexpected token {}", t))),
            None => Err(Error::Parse(format!("[Parser] unexpected end of input"))),
        }
    }

    /**
     * 解析ddl类型
     */
    fn parse_ddl(&mut self) -> Result<ast::Statement> {
        match self.next()? {
            Token::Keyword(Keyword::Create) => match self.next()? {
                Token::Keyword(Keyword::Table) => self.parse_ddl_create_table(),
                token => Err(Error::Parse(format!("[Parser] unexpected token {}", token))),
            },
            token => Err(Error::Parse(format!("[Parser] unexpected token {}", token))),
        }
    }

    /**
     * 解析create table
     */
    fn parse_ddl_create_table(&mut self) -> Result<ast::Statement> {
        //标名
        let table_name = self.next_ident()?;
        //表名之后期望是括号
        self.next_expected(Token::OpenParen)?;

        //解析列信息
        let mut colunms = Vec::<Column>::new();
        loop {
            colunms.push(self.parse_ddl_column()?);
            if self.next_if_token(Token::Comma).is_none() {
                break;
            }
        }

        self.next_expected(Token::CloseParen)?;
        Ok(ast::Statement::CreateTable {
            name: table_name,
            columns: colunms,
        })
    }

    fn parse_ddl_column(&mut self) -> Result<ast::Column> {
        let mut column = Column {
            name: self.next_ident()?,
            datatype: match self.next()? {
                Token::Keyword(Keyword::Bool) | Token::Keyword(Keyword::Boolean) => {
                    DataType::Boolean
                }
                Token::Keyword(Keyword::Double) | Token::Keyword(Keyword::Float) => DataType::Float,
                Token::Keyword(Keyword::String)
                | Token::Keyword(Keyword::Text)
                | Token::Keyword(Keyword::Varchar) => DataType::String,
                Token::Keyword(Keyword::Integer) | Token::Keyword(Keyword::Int) => {
                    DataType::Integer
                }
                token => return Err(Error::Parse(format!("[Parser] Expected token {}", token))),
            },
            nullable: None,
            default: None,
        };

        //解析列的默认值, 以及是否可以为空
        while let Some(Token::Keyword(keyword)) = self.next_if_keywork() {
            match keyword {
                Keyword::Null => column.nullable = Some(true),
                Keyword::Not => {
                    self.next_expected(Token::Keyword(Keyword::Null))?;
                    column.nullable = Some(false)
                }
                Keyword::Default => column.default = Some(self.parse_expression()?),
                k => return Err(Error::Parse(format!("[Parser] Unexpected keyword {}", k))),
            }
        }

        Ok(column)
    }

    fn parse_expression(&mut self) -> Result<ast::Expression> {
        Ok(match self.next()? {
            Token::Number(n) => {
                if n.chars().all(|it| it.is_ascii_digit()) {
                    //整型
                    ast::Consts::Integer(n.parse()?).into()
                } else {
                    //浮点型
                    ast::Consts::Float(n.parse()?).into()
                }
            }
            Token::String(v) => ast::Consts::String(v).into(),
            Token::Keyword(Keyword::True) => ast::Consts::Boolean(true).into(),
            Token::Keyword(Keyword::False) => ast::Consts::Boolean(false).into(),
            Token::Keyword(Keyword::Null) => ast::Consts::Null.into(),
            t => {
                return Err(Error::Parse(format!(
                    "[Parser] Unexpected expression token {}",
                    t
                )))
            }
        })
    }

    fn next_ident(&mut self) -> Result<String> {
        match self.next()? {
            Token::Ident(ident) => Ok(ident),
            token => Err(Error::Parse(format!(
                "[Parser] Expected ident, got token {}",
                token
            ))),
        }
    }

    /**
     * 判断下一个值是否期待值
     */
    fn next_expected(&mut self, expected: Token) -> Result<()> {
        let token = self.next()?;
        if token != expected {
            return Err(Error::Parse(format!(
                "[Parser] Expected token {}, got {}",
                expected, token
            )));
        }
        Ok(())
    }

    fn peek(&mut self) -> Result<Option<Token>> {
        self.lexer.peek().cloned().transpose()
    }

    fn next(&mut self) -> Result<Token> {
        self.lexer
            .next()
            .unwrap_or_else(|| Err(Error::Parse(format!("[Parser] unexpected end of input"))))
    }

    fn next_if<F: Fn(&Token) -> bool>(&mut self, predicate: F) -> Option<Token> {
        self.peek().unwrap_or(None).filter(predicate)?;
        self.next().ok()
    }

    fn next_if_keywork(&mut self) -> Option<Token> {
        self.next_if(|it| matches!(it, Token::Keyword(_)))
    }

    fn next_if_token(&mut self, token: Token) -> Option<Token> {
        self.next_if(|it| it == &token)
    }

    fn parse_select(&mut self) -> Result<ast::Statement> {
        self.next_expected(Token::Keyword(Keyword::Select))?;
        self.next_expected(Token::Asterisk)?;
        self.next_expected(Token::Keyword(Keyword::From))?;

        let table_name = self.next_ident()?;
        Ok(ast::Statement::Select {
            table_name: table_name,
        })
    }

    fn parse_insert(&mut self) -> Result<ast::Statement> {
        self.next_expected(Token::Keyword(Keyword::Insert))?;
        self.next_expected(Token::Keyword(Keyword::Into))?;

        //表名
        let table_name = self.next_ident()?;

        //查看是否有指定列, 有则获取列名
        let columns = if self.next_if_token(Token::OpenParen).is_some() {
            let mut cols = Vec::new();
            loop {
                cols.push(self.next_ident()?);
                match self.next()? {
                    Token::CloseParen => break,
                    Token::Comma => {}
                    token => return Err(Error::Parse(format!("[Parser] unexpected end of input"))),
                }
            }

            Some(cols)
        } else {
            None
        };

        //解析values信息
        self.next_expected(Token::Keyword(Keyword::Values))?;
        //insert into tbl values(1,2,3),(4,5,6)
        let mut values = Vec::new();
        loop {
            self.next_expected(Token::OpenParen)?;
            let mut exprs = Vec::new();
            loop {
                exprs.push(self.parse_expression()?);
                match self.next()? {
                    Token::CloseParen => break,
                    Token::Comma => {}
                    token => return Err(Error::Parse(format!("[Parser] unexpected end of input"))),
                }
            }
            values.push(exprs);

            if self.next_if_token(Token::Comma).is_none() {
                break;
            }
        }

        Ok(ast::Statement::Insert {
            table_name,
            columns,
            values,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::error::Result;

    use super::Parser;

    #[test]
    fn test_parse_crate_ddl() -> Result<()> {
        let sql1 = "
            create table tbl1 (
                a int default 100,
                b float not null,
                c varchar null,
                d bool default true
            );
        ";

        let stmt1 = Parser::new(&sql1).parse()?;
        // println!("{:?}",stmt1);

        let sql2 = "
        create table tbl1 (
            a int      default 100,
            b float  not null,
            c    varchar null,
            d bool default    true
        );
    ";

        let stmt2 = Parser::new(&sql2).parse()?;

        assert_eq!(stmt1, stmt2);

        let sql3 = "
        create table tbl1 (
            a int      default 100,
            b float  not null,
            c    varchar null,
            d bool default    true
        )
    ";

        let stmt3 = Parser::new(&sql3).parse();

        assert!(stmt3.is_err());

        Ok(())
    }

    #[test]
    fn test_parse_insert_ddl() -> Result<()> {
        let sql1 = "insert into tbl values(1,2,3,'a',true);";
        let stmt1 = Parser::new(&sql1).parse();
        assert!(stmt1.is_ok());

        let sql2 = "insert into tb2(c1,c2,c3) values(1,2,3),(4,5,6);";
        let stmt2 = Parser::new(&sql2).parse();
        assert!(stmt2.is_ok());

        Ok(())
    }

    #[test]
    fn test_parser_select_ddl() -> Result<()> {
        let sql1 = "select * from tbl1;";
        let stmt1 = Parser::new(&sql1).parse();
        assert!(stmt1.is_ok());

        Ok(())
    }
}
