use std::collections::HashMap;
use super::schema::TableDescriptor;

#[derive(Debug)]
pub struct TableBackingStore {
    pub table_name: String,
    pub id_counter: u64,
    pub mem: Vec<u8>
}

impl TableBackingStore {
    pub fn new(table_descriptor: &TableDescriptor) -> TableBackingStore {
        TableBackingStore {
            table_name: table_descriptor.table_name.to_string(),
            id_counter: 0,
            mem: Vec::new()
        }
    }

    pub fn insert_bytes_as_row(&mut self, descriptor: &TableDescriptor, bytes: &[u8]) -> Result<(), String> {
        if bytes.len() != descriptor.total_row_size() {
            Err("invalid table insertion".to_owned())
        } else {
            self.mem.extend(bytes);
            Ok(())
        }
    }

    pub fn insert(&mut self, descriptor: &TableDescriptor, columns: &[(&str, &str)]) -> Result<(), String> {
        let id = self.id_counter;
        let ins_bytes = descriptor.get_insertion_bytes(id, columns)?;
        self.id_counter += 1;

        self.insert_bytes_as_row(descriptor, &ins_bytes[..])
    }
}