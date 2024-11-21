use crate::sql::{parser::ast, schema::{self, Table},types::Value};

use super::{Node, Plan};


pub struct Planner;

impl Planner {
    pub fn new() -> Self{
        Self{}
    }

    pub fn build(&mut self,stmt : ast::Statement) -> Plan{
        Plan(self.build_statement(stmt))
    }

    fn build_statement(&self,stmt:ast::Statement) -> Node{
        match stmt {
            ast::Statement::CreateTable { name, columns } => 
                Node::CreateTable { schema: Table{
                    name:name,
                    columns:columns.into_iter().map(|it| {
                        let nullable = it.nullable.unwrap_or(true);
                        let default = match it.default {
                            Some(expr) => Some(Value::from_expression(expr)),
                            None if nullable => Some(Value::Null),
                            None => None
                        };
                        schema::Column{
                            name : it.name,
                            datatype : it.datatype,
                            nullable,
                            default
                        }
                    }).collect()
                 } },
            ast::Statement::Insert { table_name, columns, values } => 
                Node::Insert { table_name, columns: columns.unwrap_or_default(), values },
            ast::Statement::Select { table_name } => Node::Scan { table_name } ,
        }
    }
}