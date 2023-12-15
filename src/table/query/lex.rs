use std::{iter::{Peekable, Map}, cell::Cell, ops::Range};

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
    String(String),
    Number(String)
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
                let ss = self.next_alphabetic_string();        
                self.advance_while(|c| c.is_alphabetic());

                Some(Ok(TryInto::<KeywordToken>::try_into(ss)
                    .map(|kw| kw.into())
                    .unwrap_or_else(|_| QueryToken::String(ss.to_string()))))
            } else if fc.is_numeric() {
                let r = self.range_while(|c| c.is_numeric());
                Some(Ok(QueryToken::Number(self.token_string[r].to_string())))
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

impl RawSelectQuery<'_> {
    pub fn parse_string(query: &str) -> Result<RawSelectQuery<'_>, ParsingError> {
        let mut parser = TokenParser::new(query);
        parser.consume_a_keyword(KeywordToken::Select)?;
        let mut columns: Vec<RawSelectQueryColumn> = Vec::new();

        while columns.len() == 0 || parser.maybe_consume_a_character(CharacterToken::Comma)? {
            columns.push(RawSelectQuery::parse_query_column(&mut parser)?);
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
            let column = RawSelectQuery::parse_column_reference(&mut parser)?;
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