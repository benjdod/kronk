use std::collections::HashMap;

use itertools::Itertools;

use super::{schema::{DatabaseDescriptor, TableDescriptor, GetTableDescriptor}, store::TableBackingStore, query::SelectQuery};

#[derive(Debug)]
pub struct Database {
    descriptor: DatabaseDescriptor,
    table_stores: HashMap<String, TableBackingStore>
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
        self.table_stores.insert(n, TableBackingStore::new(&descriptor));
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

        backing_store.mem
            .chunks(query.table.total_size())
            .filter_map(|bytes| {
                let id_column = query.table.id_column();
                let row_id: u64 = str::parse(id_column.datatype.parse_bytes(&bytes[id_column.offset..]).unwrap().as_str()).unwrap();

                let where_cond = match &query.where_predicate {
                    Some(predicate) => predicate.conditions[..].into_iter()
                        .all(|wc| wc.comparison.is_true(&bytes[wc.column.offset..])),
                    None => true
                };

                if !where_cond { return None }

                let column_data = query.columns[..].into_iter()
                    .map(|c| (c.name.to_owned(), c.datatype.parse_bytes(&bytes[c.offset..]).unwrap()))
                    .collect_vec();

                Some((row_id, column_data))
            }).collect()
    }
}