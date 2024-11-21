use super::Executor;

pub struct Scan {
    table_name: String,
}

impl Scan {
    pub fn new(table_name: String) -> Box<Self> {
        Box::new(Scan { table_name })
    }
}

impl Executor for Scan {
    fn execute(&self) -> crate::error::Result<super::ResultSet> {
        todo!()
    }
}
