use std::{fs::{File, OpenOptions, ReadDir}, path::{Path, PathBuf}, io::{Write, BufReader}, io::prelude::*};

use super::{schema::TableDescriptor, bytes::ToNativeType};

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
    fn insert(&mut self, descriptor: &TableDescriptor, columns: &[(&str, &str)]) -> Result<(), String>;

    fn get_reader<'a>(&'a self) -> Box<dyn Read + 'a>;
}

impl ByteStore for InMemoryByteStore {
    fn insert(&mut self, descriptor: &TableDescriptor, columns: &[(&str, &str)]) -> Result<(), String> {
        let id = self.id_counter;
        let bytes = descriptor.get_insertion_bytes(id, columns)?;
        self.id_counter += 1;

        if bytes.len() != descriptor.total_row_size() {
            Err("invalid table insertion".to_owned())
        } else {
            self.mem.extend(bytes);
            Ok(())
        }
    }

    fn get_reader<'a>(&'a self) -> Box<dyn Read + 'a> {
        Box::new(std::io::BufReader::new(self.mem.as_slice()))
    }
}

pub struct FileByteStore {
    pub table_name: String,
    pub table_path: PathBuf,
    pub id_counter: u64
}

impl FileByteStore {
    pub fn new(table_descriptor: &TableDescriptor) -> std::io::Result<FileByteStore> {
        std::fs::create_dir_all(KRONKSTORE_TABLES_DIR).or_else(|e| match e.kind() {
            std::io::ErrorKind::AlreadyExists => Ok(()),
            _ => Err(e)
        })?;
        let table_path = Path::new(KRONKSTORE_TABLES_DIR).join(table_descriptor.table_name.as_str());
        dbg!(&table_path);

        if !table_path.exists() {
            let mut f = OpenOptions::new().write(true).create(true).open(&table_path)?;

            // write out the 64-byte header section, all zeroed out
            let b = [0u8; 64];
            f.write(&b)?;
        }

        Ok(FileByteStore {
            table_name: table_descriptor.table_name.to_string(),
            table_path,
            id_counter: 0
        })
    }

    pub fn get_file(&self, options: &OpenOptions) -> std::io::Result<File> {
        options.open(&self.table_path)
    }

    pub fn get_id_counter(&self, table_file: &mut File) -> std::io::Result<u64> {
        table_file.rewind()?;
        let mut id_buf = [0u8; 8];
        table_file.read_exact(id_buf.as_mut_slice())?;
        Ok(id_buf.to_native_type().unwrap())
    }

    pub fn set_id_counter(&self, table_file: &mut File, id: u64) -> std::io::Result<()> {
        table_file.rewind()?;
        let b = id.to_le_bytes();
        table_file.write(b.as_slice())?;
        Ok(())
    }
}

impl ByteStore for FileByteStore {
    fn insert(&mut self, descriptor: &TableDescriptor, columns: &[(&str, &str)]) -> Result<(), String> {
        let mut f = self.get_file(OpenOptions::new().read(true).write(true)).map_err(|_| "failed opening table file!".to_owned())?;
        let id = self.get_id_counter(&mut f).map_err(|_| "could not get id".to_owned())?;

        let bytes = descriptor.get_insertion_bytes(id, columns)?;

        if bytes.len() != descriptor.total_row_size() {
            return Err("invalid table insertion".to_owned());
        }

        f.seek(std::io::SeekFrom::End(0)).map_err(|_| "could not seek to end for appending")?;
        f.write_all(bytes.as_slice()).map_err(|_| "failed writing row to file".to_owned())?;
        self.set_id_counter(&mut f, id + 1);
        Ok(())
    }

    fn get_reader(&self) -> Box<dyn Read> {
        let mut f = File::open(&self.table_path).unwrap();
        f.seek(std::io::SeekFrom::Start(64)).unwrap();
        Box::new(BufReader::new(f))
    }
}