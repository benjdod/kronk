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

    #[error("UnexpectedEndOfInput")]
    UnexpectedEndOfInput,

    #[error("Unexpected character {0}")]
    UnexpectedCharacter(char),

    #[error("Invalid escape character {0}")]
    InvalidEscapeCharacter(char)
}