use std::result;

use crate::{
    error::{Error, Result},
    sql::{
        engine::Transaction,
        parser::ast::Expression,
        schema::Table,
        types::{Row, Value},
    },
};

use super::Executor;

pub struct Insert {
    table_name: String,
    columns: Vec<String>,
    values: Vec<Vec<Expression>>,
}

impl Insert {
    pub fn new(
        table_name: String,
        columns: Vec<String>,
        values: Vec<Vec<Expression>>,
    ) -> Box<Self> {
        Box::new(Insert {
            table_name,
            columns,
            values,
        })
    }
}

impl Insert {
    //列对其
    //insert into tbl values(1,2,3);
    //a    b    c   d
    //1    2    3   default
    fn pad_row(table: &Table, row: &Row) -> Result<Row> {
        let mut result = row.clone();
        for column in table.columns.iter().skip(row.len()) {
            if let Some(default) = &column.default {
                result.push(default.clone());
            } else {
                return Err(Error::Internal(format!(
                    "no default value for column {}",
                    column.name
                )));
            }
        }

        Ok(result)
    }
}

impl<T: Transaction> Executor<T> for Insert {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<super::ResultSet> {
        //先取出表信息
        let table = txn.must_get_table(self.table_name);
        for exprs in self.values {
            //表达式转换为value
            let value = exprs
                .into_iter()
                .map(|it| Value::from_expression(it))
                .collect::<Vec<_>>();

            //如果没有指定插入的列
            if self.columns.is_empty() {}
        }
        todo!()
    }
}
