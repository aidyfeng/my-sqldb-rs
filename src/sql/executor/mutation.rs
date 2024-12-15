use std::collections::HashMap;

use crate::{
    error::{Error, Result},
    sql::{
        engine::Transaction,
        parser::ast::Expression,
        schema::Table,
        types::{Row, Value},
    },
};

use super::{Executor, ResultSet};

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
}

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

//tbl
//insert tbl(d,c) values(1,2);
//a    b    c    d
//          2    1
fn make_row(table: &Table, column: &Vec<String>, values: &Row) -> Result<Row> {
    //判断列数是否和value数一致
    if column.len() != values.len() {
        return Err(Error::Internal(format!("columns and values num mismatch")));
    }

    let mut inputs = HashMap::new();
    for (i, column_name) in column.iter().enumerate() {
        inputs.insert(column_name, values[i].clone());
    }

    let mut results = Vec::new();
    for col in &table.columns {
        if let Some(value) = inputs.get(&col.name) {
            results.push(value.clone());
        } else if let Some(value) = &col.default {
            results.push(value.clone());
        } else {
            return Err(Error::Internal(format!(
                "not value given for the column {}",
                col.name
            )));
        }
    }

    Ok(results)
}

impl<T: Transaction> Executor<T> for Insert {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        let mut count = 0;

        //先取出表信息
        let table = txn.must_get_table(self.table_name.clone())?;
        for exprs in self.values {
            //表达式转换为value
            let row = exprs
                .into_iter()
                .map(|it| Value::from_expression(it))
                .collect::<Vec<_>>();

            let insert_row = if self.columns.is_empty() {
                pad_row(&table, &row)?
                //如果没有指定插入的列
            } else {
                //指定插入的列,需要对value信息进行整理
                make_row(&table, &self.columns, &row)?
            };
            txn.create_row(self.table_name.clone(), insert_row)?;
            count += 1;
        }

        Ok(ResultSet::Insert { count })
    }
}
