use planner::Planner;

use crate::error::Result;

use super::{
    engine::Transaction,
    executor::{Executor, ResultSet},
    parser::ast::{self, Expression},
    schema::Table,
};

mod planner;

#[derive(Debug, PartialEq)]
pub enum Node {
    //创建表
    CreateTable {
        schema: Table,
    },

    //插入数据
    Insert {
        table_name: String,
        columns: Vec<String>,
        values: Vec<Vec<Expression>>,
    },

    //扫描节点
    Scan {
        table_name: String,
    },
}

//执行计划定义, 底层是不同类型的执行节点
#[derive(Debug, PartialEq)]
pub struct Plan(pub Node);

impl Plan {
    pub fn build(stmt: ast::Statement) -> Self {
        Planner::new().build(stmt)
    }

    pub fn execute<T: Transaction>(self, txn: &mut T) -> Result<ResultSet> {
        <dyn Executor>::build(self.0).execute()
    }
}

#[cfg(test)]
mod test {
    use crate::{
        error::Result,
        sql::{parser::Parser, plan::Plan},
    };

    #[test]
    fn test_plan_create_table() -> Result<()> {
        let sql1 = "
            create table tbl1 (
                a int default 100,
                b float not null,
                c varchar null,
                d bool default true
            );
        ";

        let stmt1 = Parser::new(&sql1).parse()?;
        let p1 = Plan::build(stmt1);
        // println!("{:?}",p1);

        let sql2 = "
        create table tbl1 (
            a int      default 100,
            b float  not null,
            c    varchar null,
            d bool default    true
        );
    ";

        let stmt2 = Parser::new(&sql2).parse()?;
        let p2 = Plan::build(stmt2);

        assert_eq!(p1, p2);

        Ok(())
    }

    #[test]
    fn test_plan_insert() -> Result<()> {
        let sql1 = "insert into tbl values(1,2,3,'a',true);";
        let stmt1 = Parser::new(&sql1).parse()?;
        let p1 = Plan::build(stmt1);
        println!("{:?}", p1);
        // assert!(stmt1.is_ok());

        let sql2 = "insert into tb2(c1,c2,c3) values(1,2,3),(4,5,6);";
        let stmt2 = Parser::new(&sql2).parse()?;
        let p2 = Plan::build(stmt2);
        println!("{:?}", p2);
        // assert!(stmt2.is_ok());

        Ok(())
    }

    #[test]
    fn test_plan_select() -> Result<()> {
        let sql = "select * from tbl1;";
        let stmt = Parser::new(&sql).parse()?;
        let p1 = Plan::build(stmt);
        println!("{:?}", p1);
        Ok(())
    }
}
