use std::{fs::{File, OpenOptions}, path::{Path, PathBuf}, io::Write};

use super::schema::TableDescriptor;

const KRONKSTORE_DIRECTORY: &str = "./.kronkstore";
const KRONKSTORE_TABLES_DIR: &str = "./.kronkstore/tables";


#[derive(Debug)]
pub struct InMemoryByteStore {
    pub table_name: String,
    pub id_counter: u64,
    pub mem: Vec<u8>
}

impl InMemoryByteStore {
    pub fn new(table_descriptor: &TableDescriptor) -> InMemoryByteStore {
        InMemoryByteStore {
            table_name: table_descriptor.table_name.to_string(),
            id_counter: 1,
            mem: Vec::new()
        }
    } 
}

pub trait ByteStore {
    fn insert_bytes_as_row(&mut self, descriptor: &TableDescriptor, bytes: &[u8]) -> Result<(), String>;

    fn id_counter(&self) -> u64;

    fn increment_id_counter(&mut self);

    fn insert(&mut self, descriptor: &TableDescriptor, columns: &[(&str, &str)]) -> Result<(), String> {
        let id = self.id_counter();
        let ins_bytes = descriptor.get_insertion_bytes(id, columns)?;
        self.insert_bytes_as_row(descriptor, &ins_bytes[..])?;
        self.increment_id_counter();

        Ok(())
    }

    fn read_all<'a>(&'a self) -> Result<&'a[u8], String>;
}

impl ByteStore for InMemoryByteStore {
    fn insert_bytes_as_row(&mut self, descriptor: &TableDescriptor, bytes: &[u8]) -> Result<(), String> {
        if bytes.len() != descriptor.total_row_size() {
            Err("invalid table insertion".to_owned())
        } else {
            self.mem.extend(bytes);
            Ok(())
        }
    }

    fn insert(&mut self, descriptor: &TableDescriptor, columns: &[(&str, &str)]) -> Result<(), String> {
        let id = self.id_counter;
        let ins_bytes = descriptor.get_insertion_bytes(id, columns)?;
        self.id_counter += 1;

        self.insert_bytes_as_row(descriptor, &ins_bytes[..])
    }

    fn id_counter(&self) -> u64 {
        self.id_counter
    }

    fn increment_id_counter(&mut self) {
        self.id_counter += 1;
    }

    fn read_all<'a>(&'a self) -> Result<&'a[u8], String> {
        Ok(&self.mem)
    }
}

pub struct FileByteStore {
    pub table_name: String,
    pub table_path: PathBuf,
    pub id_counter: u64
}

impl FileByteStore {
    pub fn new(table_descriptor: &TableDescriptor) -> std::io::Result<FileByteStore> {
        let table_path = Path::new(KRONKSTORE_TABLES_DIR).join(table_descriptor.table_name.as_str());
        OpenOptions::new().create(true).open(&table_path)?;
        Ok(FileByteStore {
            table_name: table_descriptor.table_name.to_string(),
            table_path,
            id_counter: 0
        })
    }

    pub fn get_file(&self, options: &OpenOptions) -> std::io::Result<File> {
        options.open(&self.table_path)
    }
}

impl ByteStore for FileByteStore {
    fn insert_bytes_as_row(&mut self, descriptor: &TableDescriptor, bytes: &[u8]) -> Result<(), String> {
        if bytes.len() != descriptor.total_row_size() {
            return Err("invalid table insertion".to_owned());
        }

        let mut f = self.get_file(&OpenOptions::new().append(true)).map_err(|_| "failed opening table file".to_owned())?;
        f.write_all(bytes).map_err(|_| "failed writing row to file".to_owned())?;
        Ok(())
    }

    fn id_counter(&self) -> u64 {
        self.id_counter
    }

    fn increment_id_counter(&mut self) {
        self.id_counter += 1
    }

    fn read_all<'a>(&'a self) -> Result<&'a[u8], String> {
        let f = self.get_file(&OpenOptions::new().read(true)).map_err(|_| "failed opening table file for reading".to_owned())?;
        Err("agh".to_owned())
    }
}