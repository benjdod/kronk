use std::any::type_name;

use itertools::Itertools;
use uuid::Uuid;
use super::bytes::{FromSlice};

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum ColumnDataType {
    SerialId,
    Byte(usize),
    Boolean,
    Int32,
    UInt32,
    Int64,
    UInt64,
    UuidV4
}

impl ColumnDataType {
    pub fn size_in_bytes(&self) -> usize {
        let s = self;
        match s {
            Self::SerialId => 8,
            Self::Byte(u) => *u,
            Self::Boolean => 1,
            Self::Int32 => 4,
            Self::UInt32 => 4,
            Self::Int64 => 8,
            Self::UInt64 => 8,
            Self::UuidV4 => 128
        }
    }

    pub fn parse_string<'a>(&self, s: &str) -> Result<Vec<u8>, String> {
        let expected = self;
        match expected {
            Self::SerialId => Err("Cannot provide an argument for serial ids".to_owned()),
            Self::Boolean => match s {
                "true" => Ok(vec![1u8]),
                "false" => Ok(vec![0u8]),
                _ => Err(format!("Could not parse {} to a boolean", s))
            },
            Self::Int32 => str::parse::<i32>(s)
                .map(|i| i.to_le_bytes().into_iter().collect::<Vec<_>>())
                .map_err(|_| format!("Could not parse {} to an {}", s, type_name::<i32>())),
            Self::UInt32 => str::parse::<u32>(s)
                .map(|i| i.to_le_bytes().into_iter().collect::<Vec<_>>())
                .map_err(|_| format!("Could not parse {} to an {}", s, type_name::<u32>())),
            Self::Int64 => str::parse::<i64>(s)
                .map(|i| i.to_le_bytes().into_iter().collect::<Vec<_>>())
                .map_err(|_| format!("Could not parse {} to an {}", s, type_name::<i64>())),
            Self::UInt64 => str::parse::<u64>(s)
                .map(|i| i.to_le_bytes().into_iter().collect::<Vec<_>>())
                .map_err(|_| format!("Could not parse {} to an {}", s, type_name::<u64>())),

            Self::UuidV4 => str::parse::<uuid::Uuid>(s)
                .map(|i| i.as_bytes().into_iter().map(|b| *b).collect::<Vec<_>>())
                .map_err(|_| format!("Could not parse {} to a {}", s, type_name::<Uuid>())),

            Self::Byte(i) => {
                let s_bytes_len = s.as_bytes().len();
                if s_bytes_len >= (*i - 1) { Err(format!("Could not add string as Byte({}) because it's too long! ({})", i, s_bytes_len)) }
                else { Ok(s.as_bytes().into_iter().map(|b| *b).chain(std::iter::repeat(0u8).take(i - s_bytes_len)).collect::<Vec<_>>()) }
            }
        }
    }

    fn from_bytes_to_string<T>(buf: &[u8]) -> Result<String, String>
    where T: FromSlice + ToString {
        T::from_byte_buffer(buf)
            .map(|t| t.to_string())
            .map_err(|_| format!("Could not parse byte buffer to {}", type_name::<T>()))
    }

    pub fn parse_bytes(&self, bytes: &[u8]) -> Result<String, String> {
        match self {
            Self::SerialId => Self::from_bytes_to_string::<u64>(bytes),
            Self::UuidV4 => {
                let sized_bytes: [u8; 16] = bytes[..16].try_into().map_err(|_| "Byte buffer not long enough for uuid")?;
                Ok(Uuid::from_bytes(sized_bytes).to_string())
            },
            Self::Int32 => Self::from_bytes_to_string::<i32>(bytes),
            Self::UInt32 => Self::from_bytes_to_string::<u32>(bytes),
            Self::Int64 => Self::from_bytes_to_string::<i64>(bytes),
            Self::UInt64 => Self::from_bytes_to_string::<u64>(bytes),
            Self::Boolean => {
                let sized_bytes: [u8; 1] = bytes[..1].try_into()
                    .map_err(|_| "Insufficient byte buffer size for u8".to_string())?;

                Ok((sized_bytes[0] != 0).to_string())
            },
            Self::Byte(max_length) => {
                if bytes.len() < *max_length { return Err("Insufficient byte buffer size".to_string())}
                
                let s = String::from_utf8(bytes.into_iter().map(|b| *b).take_while(|b| *b != 0u8).collect_vec())
                    .map_err(|_| "could not parse byte buffer to a valid utf-8 string")?;

                Ok(s)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableColumn {
    pub name: String,
    pub datatype: ColumnDataType,
    pub offset: usize
}

#[derive(Debug)]
pub struct TableDescriptor {
    pub table_name: String,
    pub columns: Vec<TableColumn>
}

#[derive(Debug)]
pub struct DatabaseDescriptor {
    pub db_name: String,
    pub tables: Vec<TableDescriptor>
}

impl DatabaseDescriptor {
    pub fn new(name: &str, tables: Vec<TableDescriptor>) -> Result<DatabaseDescriptor, String> {
        // TODO: assert unique table names
        Ok(DatabaseDescriptor {
            db_name: name.to_owned(),
            tables
        })
    }

    pub fn add_table(&mut self, table: TableDescriptor) -> Result<(), String> {
        let t = &self.tables;
        if t.into_iter().any(|t| t.table_name == table.table_name) {
            return Err(format!("Cannot add table with duplicate name '{}'", table.table_name));
        }
        self.tables.push(table);
        Ok(())
    }
}

pub trait GetTableDescriptor {
    fn table_with_name<'a>(&'a self, table_name: &str) -> Option<&'a TableDescriptor>;
}

impl GetTableDescriptor for DatabaseDescriptor {
    fn table_with_name<'a>(&'a self, table_name: &str) -> Option<&'a TableDescriptor> {
        (&self.tables).into_iter().find(|t| t.table_name == table_name)
    }
}

impl TableDescriptor {
    pub fn new(name: &str, columns: Vec<(&str, ColumnDataType)>) -> Result<TableDescriptor, String> {
        let mut offset = 0usize;

        if columns[..].into_iter().filter(|c| c.1 == ColumnDataType::SerialId).count() != 1 {
            return Err("Table descriptor requires exactly 1 serial id".to_string());
        }

        let cols: Vec<TableColumn> = columns.into_iter()
            .map(|c| {
                let tc = TableColumn { name: c.0.to_owned(), offset: offset, datatype: c.1 };
                offset += tc.datatype.size_in_bytes();

                tc
            }).collect();

        Ok(TableDescriptor { table_name: name.to_owned(), columns: cols })
    }

    pub fn total_size(&self) -> usize {
        let cols = &self.columns;
        cols.into_iter().map(|c| c.datatype.size_in_bytes()).sum()
    }

    pub fn id_column<'a>(&'a self) -> &'a TableColumn {
        let columns = &self.columns;
        columns.into_iter().find(|c| c.datatype == ColumnDataType::SerialId).unwrap()
    }

    pub fn column_for_name<'a>(&'a self, name: &str) -> Option<&'a TableColumn> {
        let columns = &self.columns;
        columns.into_iter().find(|c| c.name == name)
    }

    pub fn get_insertion_bytes(&self, id: u64, columns: &[(&str, &str)]) -> Result<Vec<u8>, String> {
        let mut o: Vec<u8> = Vec::new();

        let dtc_columns = &self.columns;
        let mm = dtc_columns.into_iter()
            .map(|c| (c, columns.into_iter().find(|cc| cc.0 == c.name)));

        for (dtc, arg_c) in mm {
            if dtc.datatype == ColumnDataType::SerialId {
                o.extend(id.to_le_bytes());
            } else {
                match arg_c {
                    Some((_, arg)) => {
                        let parsed = dtc.datatype.parse_string(arg)?;
                        o.extend(parsed);
                    },
                    None => {
                        o.extend(std::iter::repeat(0u8).take(dtc.datatype.size_in_bytes())) 
                    }
                }
            }
        }

        Ok(o)
    }
}