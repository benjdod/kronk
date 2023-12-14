use std::iter::{Peekable, Map};

pub struct RawSelectQuery<'a> {
    pub table_name: String,
    pub columns: Vec<RawSelectQueryColumn>,
    pub where_expression: Option<RawSelectQueryWhereExpression<'a>>
}

pub struct RawSelectQueryColumn {
    pub column_name: String,
    pub table_identifier: Option<String>,
    pub as_name: Option<String>
}

pub enum RawSelectQueryWhereExpression<'a> {
    Single(RawSelectQueryWhereComparison),
    And(&'a RawSelectQueryWhereExpression<'a>, &'a RawSelectQueryWhereExpression<'a>),
    Or(&'a RawSelectQueryWhereExpression<'a>, &'a RawSelectQueryWhereExpression<'a>),
    Not(&'a RawSelectQueryWhereExpression<'a>)
}

pub struct RawSelectQueryWhereComparison {
    pub column: RawSelectQueryColumn,
    pub op: String,
    pub value: String
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
    Where
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum CharacterToken {
    LeftParen,
    RightParen,
    LeftBracket,
    RightBracket,
    Dot,
    Comma
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
}

impl<'a> Iterator for TokenIterator<'a> {
    type Item = Result<QueryToken, LexingError>;
    fn next(&mut self) -> Option<Self::Item> {

        if let Some(_) = self.err { return None }

        self.advance_while(|c| c.is_whitespace());

        if let Some(fc) = self.current_char() {
            let kw_match = if self.matches_keyword("select") {  Some(Ok(QueryToken::Keyword(KeywordToken::Select))) }
            else if self.matches_keyword("where") { Some(Ok(QueryToken::Keyword(KeywordToken::Where))) }
            else if self.matches_keyword("from") { Some(Ok(QueryToken::Keyword(KeywordToken::From))) }
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

    pub fn is_finished(&mut self) -> bool {
        match self.iterator.peek() {
            Some(_) => false,
            None => true
        }
    }

    pub fn expect_current_token(&mut self) -> Result<&'a QueryToken, ParsingError> {
        match self.iterator.peek() {
            Some(t) => match t {
                Ok(v) => Ok(v),
                Err(e) => Err(*e)
            },
            None => Err(ParsingError::UnexpectedEndOfInput)
        }
    }

    pub fn expect_is_keyword(&mut self) -> Result<(), ParsingError> {
        let t = self.expect_current_token()?;
        if let QueryToken::Keyword(_) = t { Ok(()) } else { Err(ParsingError::UnexpectedToken(QueryToken::Keyword(KeywordToken::From), *t)) }
    }
}

impl<'a> RawSelectQuery<'a> {
    pub fn parse_string(query: &str) -> Result<RawSelectQuery<'a>, ParsingError> {
        let parser = TokenParser::new(query);
        let mut token_iterator = TokenIterator::new(query).into_iter().map(|r| r.map_err(|e| <LexingError as Into<ParsingError>>::into(e))).into_iter().peekable();
        let select = token_iterator.next().unwrap()?;
        select.expect_keyword(KeywordToken::Select)?;
        let mut column_names: Vec<String> = Vec::new();
        let first_column = token_iterator.next().unwrap()?;
        if let QueryToken::String(s) = first_column {
            column_names.push(s)
        } else {
            first_column.expect_string()?;
        }
        let ti = token_iterator.peek();
        let table_name = token_iterator.next().unwrap()?;
        table_name.expect_string()?;
        for t in token_iterator {
            dbg!(t);
        }
        Err(ParsingError::InvalidSyntax)
    }
}