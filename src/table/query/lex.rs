use std::{iter::{Peekable, Map}, cell::Cell};

pub struct RawSelectQuery<'a> {
    pub table_name: String,
    pub table_identifier: Option<String>,
    pub columns: Vec<RawSelectQueryColumn>,
    pub where_expression: Option<RawSelectQueryWhereExpression<'a>>
}

pub struct RawSelectColumnReference {
    pub column_name: String,
    pub table_identifier: Option<String>
}

pub struct RawSelectQueryColumn {
    pub column: RawSelectColumnReference,
    pub as_name: Option<String>
}

pub enum RawSelectQueryWhereExpression<'a> {
    Single(RawSelectQueryWhereComparison),
    And(&'a RawSelectQueryWhereExpression<'a>, &'a RawSelectQueryWhereExpression<'a>),
    Or(&'a RawSelectQueryWhereExpression<'a>, &'a RawSelectQueryWhereExpression<'a>),
    Not(&'a RawSelectQueryWhereExpression<'a>)
}

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

#[derive(Debug)]
struct TokenIterator<'a> {
    pub token_string: &'a str,
    pub index: usize,
    pub err: Option<LexingError>
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeywordToken {
    Select,
    From,
    Where,
    As
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum CharacterToken {
    LeftParen,
    RightParen,
    LeftBracket,
    RightBracket,
    Dot,
    Comma,
    GreaterThan,
    GreaterEqual,
    LessThan,
    LessEqual,
    EqualEqual,
    NotEqual
}

impl CharacterToken {
    fn is_comparator(&self) -> bool {
        match self {
            Self::GreaterEqual | Self::GreaterThan | Self::LessEqual | Self::LessThan | Self::EqualEqual | Self::NotEqual => true,
            _ => false
        }
    }
}

impl TryFrom<CharacterToken> for RawSelectQueryWhereExpressionOperator {
    type Error = ParsingError;

    fn try_from(value: CharacterToken) -> Result<Self, Self::Error> {
        match value {
            GreaterThan => Ok(Self::GreaterThan),
            GreaterEqual => Ok(Self::GreaterEqual),
            LessThan => Ok(Self::LessThan),
            LessEqual => Ok(Self::LessEqual),
            EqualEqual => Ok(Self::EqualEqual),
            NotEqual => Ok(Self::NotEqual),
            _ => Err(ParsingError::UnexpectedToken(QueryToken::Character(CharacterToken::Comma), QueryToken::Character(value)))
        }
    }
}

#[derive(Debug, Clone)]
pub enum QueryToken {
    Character(CharacterToken),
    Keyword(KeywordToken),
    String(String)
}

impl QueryToken {
    fn expect_keyword(&self, expected: KeywordToken) -> Result<(), ParsingError> {
        if let QueryToken::Keyword(kw_token) = self {
            if *kw_token == expected { return Ok(()); }
        }
        Err(ParsingError::UnexpectedToken(QueryToken::Keyword(expected), self.clone()))
    }

    fn expect_character(&self, expected: CharacterToken) -> Result<(), ParsingError> {
        if let QueryToken::Character(c_token) = self {
            if *c_token == expected { return Ok(()) }
        }
        Err(ParsingError::UnexpectedToken(QueryToken::Character(expected), self.clone()))
    }

    fn expect_string(&self) -> Result<(), ParsingError> {
        if let QueryToken::String(_) = self { Ok(()) } else { Err(ParsingError::UnexpectedToken(QueryToken::String(String::from("")), self.clone()))}
    }
}

#[derive(Debug, Clone)]
pub enum ParsingError {
    Lexing(LexingError),
    UnexpectedToken(QueryToken, QueryToken),
    UnexpectedEndOfInput,
    InvalidSyntax
}

#[derive(Debug, Clone, Copy)]
pub enum LexingError {
    InvalidSyntax,
    UnexpectedEndOfInput,
    UnexpectedCharacter(char),
    InvalidEscapeCharacter(char)
}

impl From<LexingError> for ParsingError {
    fn from(value: LexingError) -> Self {
        Self::Lexing(value)
    }
}

impl<'a> TokenIterator<'a> {
    fn new(token_string: &'a str) -> TokenIterator<'a> {
        TokenIterator { token_string: token_string, index: 0usize, err: None }
    }

    fn nth_char(&self, i: usize) -> Option<char> {
        self.token_string.chars().nth(self.index + i)
    }

    fn current_char(&self) -> Option<char> {
        self.token_string.chars().nth(self.index) 
    }

    fn next_char(&self) -> Option<char> {
        self.token_string.chars().nth(self.index + 1)
    }

    fn chars_left(&self) -> usize {
        self.token_string.len() - self.index
    }

    fn matches_keyword(&self, match_str: &str) -> bool {
        match_str.chars()
            .enumerate()
            .all(|(i, c)| if let Some(rc) = self.nth_char(i) { rc == c } else { false })
        && (if let Some(c) = self.nth_char(match_str.len()) { c.is_whitespace() } else { false })
    }

    fn is_end_or_whitespace(&self) -> bool {
        if let Some(c) = self.current_char() { c.is_whitespace() } else { true }
    }

    fn advance(&mut self) {
        self.index += 1;
    }

    fn advance_by(&mut self, i: usize) {
        self.index += i;
    }

    fn advance_until(&mut self, predicate: fn (char) -> bool) {
        while self.index < self.token_string.len() {
            let cc = self.current_char();
            if cc.is_none() || predicate(cc.unwrap()) { break; }
            self.index += 1;
        }
    }

    fn advance_while(&mut self, predicate: fn(char) -> bool) {
        loop {
            let cc = self.current_char();
            if cc.is_none() || !predicate(cc.unwrap()) { break; }
            self.index += 1;
        }
    }

    fn slice(&self, i: usize) -> &str {
        let ii = if i < self.token_string.len() { i } else { self.token_string.len() };
        if self.index < ii { &self.token_string[self.index..ii] } else { &self.token_string[ii..self.index] }
    }

    fn consume_in_string(&mut self) -> Result<QueryToken, LexingError> {
        let mut esc = false;
        let mut acc = String::new();
        let i = self.index;

        while self.chars_left() > 0 {
            let oc = self.current_char();
            if let None = oc { return Err(LexingError::UnexpectedEndOfInput) }
            let c = oc.unwrap();

            if c == '"' && !esc {
                self.advance();
                return Ok(QueryToken::String(acc));
            }

            if esc {
                if c == '"' {
                    acc.push('"');
                    continue;
                } else {
                    return Err(LexingError::InvalidEscapeCharacter(c))
                }
            }

            if c == '\\' {
                esc = true;
                continue;
            } 

            acc.push(c)
        }

        return Err(LexingError::UnexpectedEndOfInput)
    }

    fn set_err(&mut self, err: LexingError) -> LexingError {
        self.err = Some(err);
        err
    }

    fn next_alphabetic_string(&mut self) -> &'a str {
        let mut ending_index = 0usize;
        let sliced = &self.token_string[self.index..];
        while let Some(c) = sliced.chars().nth(ending_index) {
            if c.is_alphabetic() { ending_index += 1; } else { break; }
        }
        &self.token_string[(self.index)..(self.index + ending_index)]
    }
}

impl<'a> Iterator for TokenIterator<'a> {
    type Item = Result<QueryToken, LexingError>;
    fn next(&mut self) -> Option<Self::Item> {

        if let Some(_) = self.err { return None }

        self.advance_while(|c| c.is_whitespace());

        if let Some(fc) = self.current_char() {
            if fc.is_alphabetic() {

            }
            let kw_match = 
                if self.matches_keyword("select") {  Some(Ok(QueryToken::Keyword(KeywordToken::Select))) }
            else if self.matches_keyword("where") { Some(Ok(QueryToken::Keyword(KeywordToken::Where))) }
            else if self.matches_keyword("from") { Some(Ok(QueryToken::Keyword(KeywordToken::From))) }
            else if self.matches_keyword("as") { Some(Ok(QueryToken::Keyword(KeywordToken::As))) }
            else { None };

            if kw_match.is_some() {
                self.advance_while(|c| c.is_alphanumeric());
                if self.is_end_or_whitespace() { kw_match } else {
                    Some(Err(LexingError::UnexpectedEndOfInput))
                }
            }
            else if fc.is_alphabetic() {
                let i = self.index;
                self.advance_while(|c| c.is_alphanumeric());
                let j = self.index;
                Some(Ok(QueryToken::String(self.token_string[i..j].to_string())))
            } else {
                match fc {
                    '"' => {
                        self.advance();
                        return Some(self.consume_in_string());
                    },
                    '(' => { self.advance(); Some(Ok(QueryToken::Character(CharacterToken::LeftParen))) },
                    ')' => { self.advance(); Some(Ok(QueryToken::Character(CharacterToken::RightParen))) },
                    '[' => { self.advance(); Some(Ok(QueryToken::Character(CharacterToken::LeftBracket))) },
                    ']' => { self.advance(); Some(Ok(QueryToken::Character(CharacterToken::RightBracket))) },
                    '.' => { self.advance(); Some(Ok(QueryToken::Character(CharacterToken::Dot))) },
                    ',' => { self.advance(); Some(Ok(QueryToken::Character(CharacterToken::Comma))) },
                    '=' | '<' | '>' | '!' => {
                        if self.next_char().is_none() { return Some(Err(LexingError::UnexpectedEndOfInput)) }
                        let sc = self.next_char().unwrap();
                        match (fc, sc) {
                            ('=', '=') => Some(Ok(QueryToken::Character(CharacterToken::EqualEqual))),
                            ('!', '=') => Some(Ok(QueryToken::Character(CharacterToken::NotEqual))),
                            ('<', '=') => Some(Ok(QueryToken::Character(CharacterToken::LessEqual))),
                            ('>', '=') => Some(Ok(QueryToken::Character(CharacterToken::GreaterEqual))),
                            ('>', _) => Some(Ok(QueryToken::Character(CharacterToken::GreaterThan))),
                            ('<', _) => Some(Ok(QueryToken::Character(CharacterToken::LessThan))),
                            _ => Some(Err(self.set_err(LexingError::UnexpectedCharacter(fc))))
                        }
                    },
                    _ => {
                        Some(Err(self.set_err(LexingError::UnexpectedCharacter(fc))))
                    }
                }
            }
        } else {
            return None
        }
    }

}

struct TokenParser<'a> {
    query: &'a str,
    iterator: Peekable<Box<dyn Iterator<Item = Result<QueryToken, ParsingError>> + 'a>>
}

impl<'a> TokenParser<'a> {
    pub fn new(query: &'a str) -> TokenParser<'a> {
        let i = TokenIterator::new(query).into_iter().map(|r| r.map_err(|e| <LexingError as Into<ParsingError>>::into(e)));
        let ib: Box<dyn Iterator<Item = Result<QueryToken, ParsingError>> + 'a> = Box::new(i);
        return TokenParser { iterator: ib.peekable(), query: query };
    }

    fn next(&mut self) {
        self.iterator.next();
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

    pub fn expect_is_keyword(&mut self) -> Result<KeywordToken, ParsingError> {
        let t = self.expect_current_token()?;
        match t {
            QueryToken::Keyword(kt) => Ok(kt),
            _ => Err(ParsingError::UnexpectedToken(QueryToken::Keyword(KeywordToken::From), t.clone())) 
        }
    }

    pub fn is_keyword(&mut self) -> bool {
        match self.expect_is_keyword() {
            Ok(_) => true,
            _ => false
        }
    }

    pub fn expect_a_keyword(&mut self, keyword: KeywordToken) -> Result<QueryToken, ParsingError> {
        let t = self.expect_current_token()?;
        match t {
            QueryToken::Keyword(kt) if kt == keyword => Ok(t),
            _ => Err(ParsingError::UnexpectedToken(QueryToken::Keyword(keyword), t.clone())) 
        }
    }

    pub fn consume_a_keyword(&mut self, keyword: KeywordToken) -> Result<QueryToken, ParsingError> {
        let t = self.expect_current_token()?;
        match t {
            QueryToken::Keyword(kt) if kt == keyword => { Ok(t) },
            _ => Err(ParsingError::UnexpectedToken(QueryToken::Keyword(keyword), t.clone())) 
        }
    }

    pub fn maybe_consume_a_keyword(&mut self, keyword: KeywordToken) -> Result<bool, ParsingError> {
        self.is_a_keyword(keyword).and_then(|_| { self.consume_token()?; Ok(true) })
    }

    pub fn is_a_keyword(&mut self, keyword: KeywordToken) -> Result<bool, ParsingError> {
        self.expect_a_keyword(keyword).map(|_| true)
    }

    pub fn expect_is_character(&mut self) -> Result<CharacterToken, ParsingError> {
        let t = self.expect_current_token()?;
        match t {
            QueryToken::Character(ct) => Ok(ct),
            _ => Err(ParsingError::UnexpectedToken(QueryToken::Keyword(KeywordToken::From), t.clone())) 
        }
    }

    pub fn is_character(&mut self) -> Result<bool, ParsingError> {
        self.expect_is_character().map(|_| true)
    }

    pub fn expect_a_character(&mut self, character: CharacterToken) -> Result<QueryToken, ParsingError> {
        let t = self.expect_current_token()?;
        match t {
            QueryToken::Character(ct) if ct == character => Ok(t),
            _ => Err(ParsingError::UnexpectedToken(QueryToken::Character(character), t.clone())) 
        }
    }

    pub fn maybe_consume_a_character(&mut self, character: CharacterToken) -> Result<bool, ParsingError> {
        self.is_a_character(character).and_then(|_| { self.consume_token()?; Ok(true) })
    }

    pub fn is_a_character(&mut self, character: CharacterToken) -> Result<bool, ParsingError> {
        self.expect_a_character(character).map(|_| true)
    }

    pub fn expect_string(&mut self) -> Result<String, ParsingError> {
        let t = self.expect_current_token()?;
        match t {
            QueryToken::String(s) => { Ok(s) },
            _ => Err(ParsingError::UnexpectedToken(QueryToken::String(String::from("")), t.clone())) 
        }
    }

    pub fn is_string(&mut self) -> Result<bool, ParsingError> {
        self.expect_string().map(|_| true)
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

impl RawSelectQuery<'_> {
    pub fn parse_string(query: &str) -> Result<RawSelectQuery<'_>, ParsingError> {
        let mut parser = TokenParser::new(query);
        parser.consume_a_keyword(KeywordToken::Select)?;
        let mut columns: Vec<RawSelectQueryColumn> = Vec::new();

        while columns.len() == 0 || parser.is_a_character(CharacterToken::Comma)? {
            parser.maybe_consume_a_character(CharacterToken::Comma)?;
            columns.push(RawSelectQuery::parse_query_column(&mut parser)?);
        }

        parser.consume_a_keyword(KeywordToken::From)?;

        let table_name = parser.consume_string()?;
        let table_identifier = if parser.is_string()? { Some(parser.consume_string()?) } else { None };

        let where_expression = if parser.maybe_consume_a_keyword(KeywordToken::Where)? {
            let column = RawSelectQuery::parse_column_reference(&mut parser)?;
            let op: RawSelectQueryWhereExpressionOperator = 
                parser.expect_is_character().and_then(|c| c.try_into())?;
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
        let column = RawSelectQuery::parse_column_reference(parser)?;
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