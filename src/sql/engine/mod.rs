use crate::error::{Error, Result};

use super::{executor::ResultSet, parser::Parser, plan::Plan, schema::Table, types::Row};

mod kv;
pub trait Engine: Clone {
    type Transaction: Transaction;

    fn begin(&self) -> Result<Self::Transaction>;

    fn session(&self) -> Result<Session<Self>> {
        Ok(Session {
            engine: self.clone(),
        })
    }
}

pub trait Transaction {
    fn commit(&self) -> Result<()>;

    fn rollback(&self) -> Result<()>;

    //创建行
    fn create_row(&mut self, table: String, row: Row) -> Result<()>;

    //扫描表
    fn scan_table(&self, table: Table) -> Result<Vec<Row>>;

    //ddl创建表相关
    fn create_table(&mut self, table: Table) -> Result<()>;

    //获取表相关信息
    fn get_table(&self, table_name: String) -> Result<Option<Table>>;
    // 必须获取表信息,否则报错
    fn must_get_table(&self, table_name: String) -> Result<Table> {
        self.get_table(table_name.clone())?
            .ok_or(Error::Internal(format!(
                "table {} does not exists",
                table_name
            )))
    }
}

//客户端session定义
pub struct Session<E: Engine> {
    engine: E,
}

impl<E: Engine> Session<E> {
    //执行客户端语句
    pub fn execute(&mut self, sql: &str) -> Result<ResultSet> {
        match Parser::new(sql).parse()? {
            stmt => {
                let mut txn = self.engine.begin()?;
                //构建plan, 执行sql语句
                match Plan::build(stmt).execute(&mut txn) {
                    Ok(result) => {
                        txn.commit()?;
                        Ok(result)
                    }
                    Err(err) => {
                        txn.rollback()?;
                        Err(err)
                    }
                }
            }
        }
    }
}
