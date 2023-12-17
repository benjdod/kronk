use std::error;

use thiserror::Error;

use super::lex::QueryToken;

#[derive(Debug, Clone, Error)]
pub enum ParsingError {
    #[error("lexing error")]
    Lexing(#[from] LexingError),

    #[error("Unexpected token: expected {0} but saw {1}")]
    UnexpectedToken(QueryToken, QueryToken),

    #[error("Unexpected end of input")]
    UnexpectedEndOfInput,

    #[error("Invalid syntax")]
    InvalidSyntax
}

#[derive(Debug, Clone, Copy, Error)]
pub enum LexingError {
    #[error("Invalid syntax")]
    InvalidSyntax,

    #[error("Unexpected end of input")]
    UnexpectedEndOfInput,

    #[error("Unexpected character {0}")]
    UnexpectedCharacter(char),

    #[error("Invalid escape character {0}")]
    InvalidEscapeCharacter(char)
}

pub enum RawDbCommand<'a> {
    Insert(RawInsertStatement),
    Select(RawSelectQuery<'a>)
}

pub struct RawInsertStatement {
    pub table_name: String,
    pub values: Vec<(String, String)>
}

#[derive(Debug)]
pub struct RawSelectQuery<'a> {
    pub table_name: String,
    pub table_identifier: Option<String>,
    pub columns: Vec<RawSelectQueryColumn>,
    pub where_expression: Option<RawSelectQueryWhereExpression<'a>>
}

#[derive(Debug)]
pub struct RawSelectColumnReference {
    pub column_name: String,
    pub table_identifier: Option<String>
}

#[derive(Debug)]
pub struct RawSelectQueryColumn {
    pub column: RawSelectColumnReference,
    pub as_name: Option<String>
}

#[derive(Debug)]
pub enum RawSelectQueryWhereExpression<'a> {
    Single(RawSelectQueryWhereComparison),
    And(&'a RawSelectQueryWhereExpression<'a>, &'a RawSelectQueryWhereExpression<'a>),
    Or(&'a RawSelectQueryWhereExpression<'a>, &'a RawSelectQueryWhereExpression<'a>),
    Not(&'a RawSelectQueryWhereExpression<'a>)
}

#[derive(Debug)]
pub struct RawSelectQueryWhereComparison {
    pub column: RawSelectColumnReference,
    pub op: RawSelectQueryWhereExpressionOperator,
    pub value: String
}


#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RawSelectQueryWhereExpressionOperator {
    GreaterThan,
    GreaterEqual,
    LessThan,
    LessEqual,
    EqualEqual,
    NotEqual
}

impl ToString for RawSelectQueryWhereExpressionOperator {
    fn to_string(&self) -> String {
        (match self {
            Self::GreaterThan => ">",
            Self::GreaterEqual => ">=",
            Self::LessThan => "<",
            Self::LessEqual => "<=",
            Self::EqualEqual => "==",
            Self::NotEqual => "!="
        }).to_owned()
    }
}