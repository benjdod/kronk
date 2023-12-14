use std::str::FromStr;

use itertools::Itertools;
use uuid::Uuid;

pub mod lex;

use super::{
    schema::{TableColumn, TableDescriptor, ColumnDataType, DatabaseDescriptor, GetTableDescriptor},
    bytes::{FromSlice}
};

#[derive(Debug)]
pub struct SelectQuery<'a> {
    pub table: &'a TableDescriptor,
    pub columns: Vec<&'a TableColumn>,
    pub where_predicate: Option<WherePredicate<'a>>
}

#[derive(Debug)]
pub struct WherePredicate<'a> {
    pub conditions: Vec<WhereCondition<'a>>
}

#[derive(Debug)]
pub struct WhereCondition<'a> {
    pub column: &'a TableColumn,
    pub comparison: WhereComparison
}

#[derive(Debug)]
enum PartialOrdOperator {
    GreaterThan,
    GreaterEqual,
    LessThan,
    LessEqual,
}

impl PartialOrdOperator {
    pub fn evaluate<T>(&self, a: &T, b: &T) -> bool 
        where T : PartialOrd
    {
        match self {
            PartialOrdOperator::GreaterThan => *a > *b,
            PartialOrdOperator::GreaterEqual => *a >= *b,
            PartialOrdOperator::LessThan => *a < *b,
            PartialOrdOperator::LessEqual => *a <= *b
        }
    }
}

impl FromStr for PartialOrdOperator {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim() {
            ">=" => Ok(Self::GreaterEqual),
            ">"  => Ok(Self::GreaterThan),
            "<=" => Ok(Self::LessEqual),
            "<"  => Ok(Self::LessThan),
            _    => Err(format!("Invalid partial ord operator {}", s))
        }
    }
}

#[derive(Debug)]
enum PartialEqOperator {
    Equal,
    NotEqual
}

impl PartialEqOperator {
    pub fn evaluate<T>(&self, a: &T, b: &T) -> bool 
        where T : PartialEq
    {
        match self {
            PartialEqOperator::Equal => *a == *b,
            PartialEqOperator::NotEqual => *a != *b
        }
    }
}

impl FromStr for PartialEqOperator {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim() {
            "==" => Ok(Self::Equal),
            "!="  => Ok(Self::NotEqual),
            _    => Err(format!("Invalid partial eq operator {}", s))
        }
    }
}

#[derive(Debug)]
enum EqOrdOperator {
    Eq(PartialEqOperator),
    Ord(PartialOrdOperator)
}

impl EqOrdOperator {
    pub fn evaluate<T>(&self, a: &T, b: &T) -> bool 
        where T : PartialEq + PartialOrd
    {
        match self {
            Self::Eq(eqc) => eqc.evaluate(a, b),
            Self::Ord(ordc) => ordc.evaluate(a, b)
        }
    }

}

impl FromStr for EqOrdOperator {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(eq_op) = str::parse::<PartialEqOperator>(s) {
            Ok(EqOrdOperator::Eq(eq_op))
        } else if let Ok(ord_op) = str::parse::<PartialOrdOperator>(s) {
            Ok(EqOrdOperator::Ord(ord_op))
        } else {
            Err(format!("invalid operator '{}'", s))
        }
    }
}

#[derive(Debug)]
pub struct EqOrdComparison<T> where T : PartialEq + PartialOrd {
    operator: EqOrdOperator,
    value: T
}

#[derive(Debug)]
pub struct EqComparison<T> where T : PartialEq {
    operator: PartialEqOperator,
    value: T
}

trait EvaluableComparison<T> {
    fn evaluate(&self, other: &T) -> bool;
}

impl<T> EvaluableComparison<T> for EqComparison<T> where T : PartialEq {
    fn evaluate(&self, other: &T) -> bool {
        let s = self;
        match s.operator {
            PartialEqOperator::Equal => s.value == *other,
            PartialEqOperator::NotEqual => s.value != *other
        }
    }
}

#[derive(Debug)]
pub enum WhereComparison {
    Int32(EqOrdComparison<i32>),
    UInt32(EqOrdComparison<u32>),
    Int64(EqOrdComparison<i64>),
    UInt64(EqOrdComparison<u64>),
    UuidV4(EqComparison<Uuid>),
    String(EqComparison<String>),
    SerialId(EqOrdComparison<u64>),
    Boolean(EqComparison<bool>)
}

impl ColumnDataType {
    fn parse_where_comparison(&self, op: &str, value: &str) -> Result<WhereComparison, String> {
        let s = self;
        match s {
            Self::Boolean => {
                let v = str::parse::<bool>(value)
                    .map_err(|_| format!("Invalid where expression: '{}' is not a boolean value", value))?;

                let parsed_op: PartialEqOperator = str::parse(op)
                    .map_err(|s| format!("Invalid where expression: {}", s))?;

                Ok(WhereComparison::Boolean(EqComparison { operator: parsed_op, value: v }))
            },

            Self::SerialId => {
                let v = str::parse::<u64>(value)
                    .map_err(|_| format!("Invalid where expression: '{}' is not a serial id", value))?;

                let parsed_op: EqOrdOperator = str::parse(op)
                    .map_err(|s| format!("Invalid where expression: {}", s))?;
                
                Ok(WhereComparison::SerialId(EqOrdComparison { operator: parsed_op, value: v }))
            },

            Self::Int32 => {
                let v = str::parse::<i32>(value)
                    .map_err(|_| format!("Invalid where expression: '{}' is not an int32 value", value))?;

                let parsed_op: EqOrdOperator = str::parse(op)
                    .map_err(|s| format!("Invalid where expression: {}", s))?;

                Ok(WhereComparison::Int32(EqOrdComparison { operator: parsed_op, value: v }))
            },

            Self::UInt32 => {
                let v = str::parse::<u32>(value)
                    .map_err(|_| format!("Invalid where expression: '{}' is not a u32 value", value))?;

                let parsed_op: EqOrdOperator = str::parse(op)
                    .map_err(|s| format!("Invalid where expression: {}", s))?;

                Ok(WhereComparison::UInt32(EqOrdComparison { operator: parsed_op, value: v }))
            },

            Self::Int64 => {
                let v = str::parse::<i64>(value)
                    .map_err(|_| format!("Invalid where expression: '{}' is not an i64 value", value))?;

                let parsed_op: EqOrdOperator = str::parse(op)
                    .map_err(|s| format!("Invalid where expression: {}", s))?;

                Ok(WhereComparison::Int64(EqOrdComparison { operator: parsed_op, value: v }))
            },

            Self::UInt64 => {
                let v = str::parse::<u64>(value)
                    .map_err(|_| format!("Invalid where expression: '{}' is not a u64 value", value))?;

                let parsed_op: EqOrdOperator = str::parse(op)
                    .map_err(|s| format!("Invalid where expression: {}", s))?;

                Ok(WhereComparison::UInt64(EqOrdComparison { operator: parsed_op, value: v }))
            },

            Self::UuidV4 => {
                let v = str::parse::<Uuid>(value)
                    .map_err(|_| format!("Invalid where expression: '{}' is not a uuid value", value))?;

                let parsed_op: PartialEqOperator = str::parse(op)
                    .map_err(|s| format!("Invalid where expression: {}", s))?;

                Ok(WhereComparison::UuidV4(EqComparison { operator: parsed_op, value: v }))
            }

            Self::Byte(_) => {
                let parsed_op: PartialEqOperator = str::parse(op)
                    .map_err(|s| format!("Invalid where expression: {}", s))?;

                Ok(WhereComparison::String(EqComparison { operator: parsed_op, value: value.to_string() }))
            }
        }
    }
}

impl WhereComparison {
    pub fn is_true(&self, buf: &[u8]) -> bool {
        let s = self;
        match s {
            Self::SerialId(comparison) => {
                let sized_buf: &[u8; 8] = buf[..8].try_into().unwrap();
                let i = u64::from_le_bytes(*sized_buf);
                comparison.operator.evaluate(&i, &comparison.value)
            }
            Self::Int32(comparison) => {
                comparison.operator.evaluate(&i32::from_slice(buf).unwrap(), &comparison.value)
            },
            Self::UInt32(comparison) => {
                comparison.operator.evaluate(&u32::from_slice(buf).unwrap(), &comparison.value)
            },
            Self::Int64(comparison) => {
                comparison.operator.evaluate(&i64::from_slice(buf).unwrap(), &comparison.value)
            },
            Self::UInt64(comparison) => {
                comparison.operator.evaluate(&u64::from_slice(buf).unwrap(), &comparison.value)
            },
            Self::UuidV4(comparison) => {
                comparison.operator.evaluate(&Uuid::from_slice(&buf[..16]).unwrap(), &comparison.value)
            }
            Self::Boolean(comparison) => {
                let b = buf[0] != 0u8;
                comparison.operator.evaluate(&b, &comparison.value)
            },
            Self::String(comparison) => {
                let s = String::from_utf8(buf.into_iter().map(|b| *b).take_while(|b| *b != 0u8).collect()).unwrap();
                comparison.operator.evaluate(&s, &comparison.value)
            }
        }
    }
}

impl<'a> SelectQuery<'a> {
    pub fn parse_query_string(query: &str, db_descriptor: &'a impl GetTableDescriptor) -> Result<SelectQuery<'a>, String> {
        let tokens = query.trim().split_whitespace().collect::<Vec<&str>>();

        if tokens.len() == 0 { return Err("Query cannot be empty".to_owned()); }

        if tokens[0] != "select" { return Err("invalid query: the only allowed query command is 'select'".to_owned());}

        let columns_ending_idx = tokens[..].into_iter()
            .take_while(|t| **t != "from")
            .count();

        if columns_ending_idx == tokens.len() { return Err("Invalid query: missing 'from'".to_owned()) }

        let select_column_names = &tokens[1..columns_ending_idx];

        let table_name = *tokens[..].into_iter()
            .skip(columns_ending_idx + 1)
            .next().unwrap();

        let table = db_descriptor.table_with_name(table_name)
            .ok_or_else(|| format!("Invalid query: no table '{}' exists", table_name))?;

        let select_columns = select_column_names.into_iter()
            .map(|t| table.column_for_name(t))
            .collect::<Vec<Option<&TableColumn>>>();

        for column in select_columns[..].into_iter() {
            if let None = column { return Err("Missing column!".to_owned()) }
        }

        let where_predicate = if select_columns.len() == tokens.len() - 1 { None } else {
            let where_conditions = tokens[(4+select_columns.len())..]
                .chunks(3)
                .map(|c| {
                    let column = c[0];
                    let op = c[1];
                    let value = c[2];
                    let table_column = table.column_for_name(column)
                        .ok_or_else(|| format!("Invalid where comparison: column {} does not exist", column))?;

                    let where_comparison = table_column.datatype.parse_where_comparison(op, value)?;

                    Ok(WhereCondition {
                        column: table_column,
                        comparison: where_comparison
                    })
                })
                .collect::<Vec<Result<WhereCondition, String>>>();

            if let Some(err) = where_conditions[..].into_iter().find(|wc| wc.is_err()) {
                return Err(err.as_ref().unwrap_err().to_owned());
            }

            Some(WherePredicate {
                conditions: where_conditions.into_iter().filter_map(|r| r.ok()).collect_vec()
            })
        };

        Ok(SelectQuery {
            table,
            columns: select_columns.into_iter().map(|scn| scn.unwrap()).collect_vec(),
            where_predicate
        })
    }
}