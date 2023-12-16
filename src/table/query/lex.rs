use std::{iter::{Peekable, Map}, cell::Cell, ops::Range, fmt::{Display, write}};
use super::types::*;

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

#[derive(Debug)]
pub struct TokenIterator<'a> {
    pub token_string: &'a str,
    pub index: usize,
    pub err: Option<LexingError>
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeywordToken {
    Select,
    From,
    Where,
    As
}

impl TryFrom<&str> for KeywordToken {
    type Error = ();
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "select" => Ok(Self::Select),
            "from" => Ok(Self::From),
            "where" => Ok(Self::Where),
            "as" => Ok(Self::As),
            _ => Err(())
        }
    }
}

impl ToStaticStr for KeywordToken {
    fn static_str(&self) -> &'static str {
        match self {
            KeywordToken::As => "as",
            KeywordToken::From => "from",
            KeywordToken::Select => "select",
            KeywordToken::Where => "where"
        }
    }
}

impl Display for KeywordToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", Self::static_str(&self))
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum CharacterToken {
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

trait ToStaticStr {
    fn static_str(&self) -> &'static str;
}

impl ToStaticStr for CharacterToken {
    fn static_str(&self) -> &'static str {
        match self {
            CharacterToken::Comma => ",",
            CharacterToken::Dot => ".",
            CharacterToken::EqualEqual => "==",
            CharacterToken::NotEqual => "!=",
            CharacterToken::GreaterEqual => ">=",
            CharacterToken::LessEqual => "<=",
            CharacterToken::LessThan => "<",
            CharacterToken::GreaterThan => ">",
            CharacterToken::LeftParen => "(",
            CharacterToken::RightParen => ")",
            CharacterToken::LeftBracket => "{",
            CharacterToken::RightBracket => "}",
        }
    }
}

impl Display for CharacterToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", Self::static_str(&self))
    }
}

impl TryFrom<CharacterToken> for RawSelectQueryWhereExpressionOperator {
    type Error = ParsingError;

    fn try_from(value: CharacterToken) -> Result<Self, Self::Error> {
        match value {
            CharacterToken::GreaterThan => Ok(Self::GreaterThan),
            CharacterToken::GreaterEqual => Ok(Self::GreaterEqual),
            CharacterToken::LessThan => Ok(Self::LessThan),
            CharacterToken::LessEqual => Ok(Self::LessEqual),
            CharacterToken::EqualEqual => Ok(Self::EqualEqual),
            CharacterToken::NotEqual => Ok(Self::NotEqual),
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

impl std::fmt::Display for QueryToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Character(c) => write!(f, "char({})", c),
            Self::Keyword(k) => write!(f, "kw({})", k),
            Self::String(s) => write!(f, "string({})", s)
        }
    }
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

impl From<KeywordToken> for QueryToken {
    fn from(kw: KeywordToken) -> Self {
        QueryToken::Keyword(kw)
    }
}


impl<'a> TokenIterator<'a> {
    pub fn new(token_string: &'a str) -> TokenIterator<'a> {
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

    fn range_while(&mut self, predicate: fn (char) -> bool) -> Range<usize> {
        let start_idx = self.index;
        self.advance_while(predicate);
        Range { start: start_idx, end: self.index }
    }

    fn range_until(&mut self, predicate: fn (char) -> bool) -> Range<usize> {
        let start_idx = self.index;
        self.advance_until(predicate);
        Range { start: start_idx, end: self.index }
    }

    fn slice(&self, i: usize) -> &str {
        let ii = if i < self.token_string.len() { i } else { self.token_string.len() };
        if self.index < ii { &self.token_string[self.index..ii] } else { &self.token_string[ii..self.index] }
    }

    fn consume_in_string(&mut self) -> Result<QueryToken, LexingError> {
        let mut esc = false;
        let mut acc = String::new();

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
                    esc = false;
                    self.advance();
                    continue;
                } else {
                    return Err(LexingError::InvalidEscapeCharacter(c))
                }
            }

            if c == '\\' {
                esc = true;
                self.advance();
                continue;
            } 

            self.advance();
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
            let cond = if ending_index > 0 {
                c.is_alphanumeric() || c == '_'
            } else { c.is_alphabetic() };

            if cond { ending_index += 1; } else { break; }
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
                let ss = self.next_alphabetic_string();        
                self.advance_by(ss.len());

                Some(Ok(TryInto::<KeywordToken>::try_into(ss)
                    .map(|kw| kw.into())
                    .unwrap_or_else(|_| QueryToken::String(ss.to_string()))))
            } else if fc.is_numeric() {
                let r = self.range_while(|c| c.is_numeric());
                Some(Ok(QueryToken::String(self.token_string[r].to_string())))
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
                        let o = match (fc, sc) {
                            ('=', '=') => Some(Ok(QueryToken::Character(CharacterToken::EqualEqual))),
                            ('!', '=') => Some(Ok(QueryToken::Character(CharacterToken::NotEqual))),
                            ('<', '=') => Some(Ok(QueryToken::Character(CharacterToken::LessEqual))),
                            ('>', '=') => Some(Ok(QueryToken::Character(CharacterToken::GreaterEqual))),
                            ('>', _) => Some(Ok(QueryToken::Character(CharacterToken::GreaterThan))),
                            ('<', _) => Some(Ok(QueryToken::Character(CharacterToken::LessThan))),
                            _ => Some(Err(self.set_err(LexingError::UnexpectedCharacter(fc))))
                        };

                        if let Some(r) = o {
                            self.advance();
                            self.advance();
                            Some(r)
                        } else { None }
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