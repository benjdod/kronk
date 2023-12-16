use std::iter::Peekable;

use super::lex::{RawSelectQuery, QueryToken, TokenIterator, RawSelectColumnReference, KeywordToken, CharacterToken, RawSelectQueryColumn, RawSelectQueryWhereExpressionOperator, RawSelectQueryWhereComparison, RawSelectQueryWhereExpression};
use super::types::{LexingError, ParsingError};

pub struct RawParse {}

impl RawParse {
    pub fn parse_string(query: &str) -> Result<RawSelectQuery<'_>, ParsingError> {
        let mut parser = TokenParser::new(query);
        parser.consume_a_keyword(KeywordToken::Select)?;
        let mut columns: Vec<RawSelectQueryColumn> = Vec::new();

        while columns.len() == 0 || parser.maybe_consume_a_character(CharacterToken::Comma)? {
            columns.push(Self::parse_query_column(&mut parser)?);
        }

        parser.consume_a_keyword(KeywordToken::From)?;

        let table_name = parser.consume_string()?;
        let table_identifier = if parser.is_finished() { None } else if parser.is_string()? { Some(parser.consume_string()?) } else { None };

        if parser.is_finished() {
            return Ok(RawSelectQuery {
                table_name,
                table_identifier,
                columns,
                where_expression: None
            })
        }

        let where_expression = if parser.maybe_consume_a_keyword(KeywordToken::Where)? {
            let column = Self::parse_column_reference(&mut parser)?;
            let op: RawSelectQueryWhereExpressionOperator = 
                parser.consume_character().and_then(|c| c.try_into())?;
            let value = parser.consume_string()?;
            let ww = RawSelectQueryWhereComparison {
                column,
                op,
                value
            };

            Some(RawSelectQueryWhereExpression::Single(ww))
        } else { 
            None
        };

        Ok(RawSelectQuery {
            table_name,
            table_identifier,
            columns,
            where_expression
        })
    }

    fn parse_query_column(parser: &mut TokenParser<'_>) -> Result<RawSelectQueryColumn, ParsingError> {
        let column = Self::parse_column_reference(parser)?;
        let as_name = if parser.is_a_keyword(KeywordToken::As)? {
            parser.consume_token()?;
            Some(parser.consume_string()?)
        } else {
            None
        };

        Ok(RawSelectQueryColumn {
            column,
            as_name
        })
    }

    fn parse_column_reference(parser: &mut TokenParser<'_>) -> Result<RawSelectColumnReference, ParsingError> {
        let s1 = parser.consume_string()?;
        let s2 = if parser.is_a_character(CharacterToken::Dot)? { 
            parser.consume_token()?;
            Some(parser.consume_string()?) 
        } else {
            None
        };

        Ok(match (s1, s2) {
            (table_identifier, Some(column_name)) => RawSelectColumnReference { table_identifier: Some(table_identifier), column_name },
            (column_name, None) => RawSelectColumnReference { table_identifier: None, column_name }
        })
    }
}

struct TokenParser<'a> {
    query: &'a str,
    iterator: Peekable<Box<dyn Iterator<Item = Result<QueryToken, ParsingError>> + 'a>>,
    current_token: Option<Result<QueryToken, ParsingError>>
}

impl<'a> TokenParser<'a> {
    pub fn new(query: &'a str) -> TokenParser<'a> {
        let i = TokenIterator::new(query).into_iter().map(|r| r.map_err(|e| <LexingError as Into<ParsingError>>::into(e)));
        let ib: Box<dyn Iterator<Item = Result<QueryToken, ParsingError>> + 'a> = Box::new(i);
        return TokenParser { iterator: ib.peekable(), query: query, current_token: None };
    }

    fn next(&mut self) {
        self.iterator.next();
        self.current_token = self.iterator.peek().map(|r| r.clone());
    }

    pub fn is_finished(&mut self) -> bool {
        match self.iterator.peek() {
            Some(_) => false,
            None => true
        }
    }

    pub fn expect_current_token(&mut self) -> Result<QueryToken, ParsingError> {
        match self.iterator.peek() {
            Some(t) => match t {
                Ok(v) => Ok(v.clone()),
                Err(e) => Err(e.clone())
            },
            None => Err(ParsingError::UnexpectedEndOfInput)
        }
    }


    // any keyword

    fn match_is_keyword(&mut self) -> Result<(Option<KeywordToken>, QueryToken), ParsingError> {
        let t = self.expect_current_token()?;
        Ok((match t {
            QueryToken::Keyword(c) => Some(c),
            _ => None
        }, t))

    }

    pub fn expect_is_keyword(&mut self) -> Result<KeywordToken, ParsingError> {
        self.match_is_keyword()
            .and_then(|(c, t)| c.ok_or_else(|| ParsingError::UnexpectedToken(QueryToken::Keyword(KeywordToken::Select), t)))
    }

    pub fn is_keyword(&mut self) -> Result<bool, ParsingError> {
        self.match_is_keyword().map(|(c, _)| c.is_some())
    }


    // a certain keyword

    fn match_is_a_keyword(&mut self, keyword: KeywordToken) -> Result<(Option<KeywordToken>, QueryToken), ParsingError> {
        let t = self.expect_current_token()?;
        Ok((match t {
            QueryToken::Keyword(c) if c == keyword => Some(c),
            _ => None
        }, t))
    }

    pub fn is_a_keyword(&mut self, keyword: KeywordToken) -> Result<bool, ParsingError> {
        self.match_is_a_keyword(keyword).map(|(c, _)| c.is_some())
    }

    pub fn expect_is_a_keyword(&mut self, keyword: KeywordToken) -> Result<(), ParsingError> {
        self.match_is_a_keyword(keyword)
            .and_then(|(c, t)| c.map(|_| ()).ok_or_else(|| ParsingError::UnexpectedToken(QueryToken::Keyword(keyword), t)))
    }

    pub fn consume_a_keyword(&mut self, keyword: KeywordToken) -> Result<(), ParsingError> {
        self.expect_is_a_keyword(keyword).and_then(|_| { self.consume_token()?; Ok(()) })
    }

    pub fn maybe_consume_a_keyword(&mut self, keyword: KeywordToken) -> Result<bool, ParsingError> {
        self.is_a_keyword(keyword).and_then(|v| { if v { self.consume_token()?; } Ok(v) })
    }


    // any character

    fn match_is_character(&mut self) -> Result<(Option<CharacterToken>, QueryToken), ParsingError> {
        let t = self.expect_current_token()?;
        Ok((match t {
            QueryToken::Character(c) => Some(c),
            _ => None
        }, t))
    }

    pub fn expect_is_character(&mut self) -> Result<CharacterToken, ParsingError> {
        self.match_is_character()
            .and_then(|(c, t)| c.ok_or_else(|| ParsingError::UnexpectedToken(QueryToken::Character(CharacterToken::Comma), t)))
    }

    pub fn is_character(&mut self) -> Result<bool, ParsingError> {
        self.match_is_character().map(|(c, _)| c.is_some())
    }

    pub fn consume_character(&mut self) -> Result<CharacterToken, ParsingError> {
        self.expect_is_character().and_then(|c| { self.consume_token()?; Ok(c) })
    }


    // a certain character

    fn match_is_a_character(&mut self, character: CharacterToken) -> Result<(Option<CharacterToken>, QueryToken), ParsingError> {
        let t = self.expect_current_token()?;
        Ok((match t {
            QueryToken::Character(c) if c == character => Some(c),
            _ => None
        }, t))
    }

    pub fn is_a_character(&mut self, character: CharacterToken) -> Result<bool, ParsingError> {
        self.match_is_a_character(character).map(|(c, _)| c.is_some())
    }

    pub fn expect_is_a_character(&mut self, character: CharacterToken) -> Result<(), ParsingError> {
        self.match_is_a_character(character)
            .and_then(|(c, t)| c.map(|_| ()).ok_or_else(|| ParsingError::UnexpectedToken(QueryToken::Character(character), t)))
    }

    pub fn consume_a_character(&mut self, character: CharacterToken) -> Result<(), ParsingError> {
        self.expect_is_a_character(character).and_then(|_| { self.consume_token()?; Ok(()) })
    }

    pub fn maybe_consume_a_character(&mut self, character: CharacterToken) -> Result<bool, ParsingError> {
        self.is_a_character(character).and_then(|v| { if v { self.consume_token()?; } Ok(v) })
    }

    fn match_is_string(&mut self) -> Result<Option<String>, ParsingError> {
        let t = self.expect_current_token()?;
        match t {
            QueryToken::String(s) => Ok(Some(s)),
            _ => Ok(None)
        }
    }

    pub fn is_string(&mut self) -> Result<bool, ParsingError> {
        self.match_is_string().map(|r| r.is_some())
    }

    pub fn expect_string(&mut self) -> Result<String, ParsingError> {
        let t = self.expect_current_token()?;
        match t {
            QueryToken::String(s) => { Ok(s) },
            _ => Err(ParsingError::UnexpectedToken(QueryToken::String(String::from("")), t.clone())) 
        }
    }

    pub fn consume_string(&mut self) -> Result<String, ParsingError> {
        let exp = self.expect_string();
        match self.expect_string() {
            Ok(s) => { self.consume_token(); Ok(s) }
            _ => exp
        }
    }

    pub fn consume_token(&mut self) -> Result<QueryToken, ParsingError> {
        self.next();
        self.expect_current_token()
    }
}