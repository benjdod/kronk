use std::collections::HashMap;
use std::io::prelude::*;
use itertools::Itertools;

use super::{schema::{DatabaseDescriptor, TableDescriptor, GetTableDescriptor}, store::{InMemoryByteStore, ByteStore, FileByteStore}, query::SelectQuery};

pub struct Database {
    descriptor: DatabaseDescriptor,
    table_stores: HashMap<String, Box<dyn ByteStore>>
}

impl Database {
    pub fn new(db_name: &str) -> Database {
        Database { 
            descriptor: DatabaseDescriptor { 
                db_name: db_name.to_owned(), 
                tables: Vec::new() 
            }, 
            table_stores: HashMap::new()
        }
    }

    pub fn add_table(&mut self, descriptor: TableDescriptor) -> Result<(), String> {
        let n = descriptor.table_name.clone();
        let fbs = FileByteStore::new(&descriptor).unwrap();
        self.table_stores.insert(n,  Box::new(fbs));
        self.descriptor.add_table(descriptor)?;

        Ok(())
    }

    pub fn insert_columns(&mut self, table_name: &str, columns: &[(&str, &str)]) -> Result<(), String> {
        let table_descriptor = self.descriptor.table_with_name(table_name)
            .ok_or_else(|| format!("No table '{}' exists", table_name))?;
        let backing_store = self.table_stores.get_mut(table_name).expect("Table backig store should be present here");
        backing_store.insert(table_descriptor, columns)
    }
}

impl GetTableDescriptor for Database {
    fn table_with_name<'a>(&'a self, table_name: &str) -> Option<&'a TableDescriptor> {
        self.descriptor.table_with_name(table_name)
    }
}

impl Database {
    pub fn query(&self, query: &SelectQuery) -> Vec<(u64, Vec<(String, String)>)> {
        let backing_store = self.table_stores.get(&query.table.table_name).expect("backing store here shold be populated");

        let row_size = query.table.total_row_size();

        let mut reader = backing_store.get_reader();
        let mut dest_vec: Vec<u8> = Vec::new();
        dest_vec.extend(std::iter::repeat(0u8).take(row_size));
        let bytes = dest_vec.as_mut_slice();

        let mut out: Vec<(u64, Vec<(String, String)>)> = vec![];

        loop {
            let bytes_read = reader.read(bytes).unwrap();
            if bytes_read == 0 { break; }
            if bytes_read != row_size { panic!("woah buddy, file size ain't right") }

            let id_column = query.table.id_column();
            let row_id: u64 = str::parse(id_column.datatype.parse_bytes(&bytes[id_column.offset..]).unwrap().as_str()).unwrap();

            let where_cond = match &query.where_predicate {
                Some(predicate) => predicate.conditions[..].into_iter()
                    .all(|wc| wc.comparison.is_true(&bytes[wc.column.offset..])),
                None => true
            };

            if !where_cond { continue; }

            let column_data = query.columns[..].into_iter()
                .map(|c| (c.name.to_owned(), c.datatype.parse_bytes(&bytes[c.offset..]).unwrap()))
                .collect_vec();

            out.push((row_id, column_data));
        }

        out
    }
}