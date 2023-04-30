#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ParseError;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Query {
    Word(String),
    And(Box<Query>, Box<Query>),
    Or(Box<Query>, Box<Query>),
}

fn primary_expr(input: &str) -> Result<(&str, Query), ParseError> {
    let input = input.trim_start();
    if let Some(input) = input.strip_prefix('(') {
        let (input, query) = expr(input)?;
        let input = input.trim_start();
        if !input.starts_with(')') {
            return Err(ParseError);
        }
        Ok((&input[1..], query))
    } else {
        let (word, input) = match input.find(|c: char| !c.is_alphanumeric()) {
            Some(idx) => input.split_at(idx),
            None => (input, ""),
        };
        Ok((input, Query::Word(word.to_string())))
    }
}

fn or_expr(input: &str) -> Result<(&str, Query), ParseError> {
    let (input, lhs) = primary_expr(input)?;
    let input = input.trim_start();
    if let Some(input) = input.strip_prefix("OR") {
        let (input, rhs) = or_expr(input)?;
        Ok((input, Query::Or(Box::new(lhs), Box::new(rhs))))
    } else {
        Ok((input, lhs))
    }
}

fn and_expr(input: &str) -> Result<(&str, Query), ParseError> {
    let (input, lhs) = or_expr(input)?;
    let input = input.trim_start();
    if let Some(input) = input.strip_prefix("AND") {
        let (input, rhs) = and_expr(input)?;
        Ok((input, Query::And(Box::new(lhs), Box::new(rhs))))
    } else {
        Ok((input, lhs))
    }
}

fn expr(input: &str) -> Result<(&str, Query), ParseError> {
    and_expr(input)
}

pub fn parse(input: &str) -> Result<Query, ParseError> {
    let (input, query) = expr(input)?;
    if input.trim_end().is_empty() {
        Ok(query)
    } else {
        Err(ParseError)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_or() {
        assert_eq!(
            parse("foo OR bar").unwrap(),
            Query::Or(
                Box::new(Query::Word("foo".to_string())),
                Box::new(Query::Word("bar".to_string()))
            )
        );

        assert_eq!(
            parse("foo OR bar OR baz").unwrap(),
            Query::Or(
                Box::new(Query::Word("foo".to_string())),
                Box::new(Query::Or(
                    Box::new(Query::Word("bar".to_string())),
                    Box::new(Query::Word("baz".to_string()))
                ))
            )
        );
    }

    #[test]
    fn test_and() {
        assert_eq!(
            parse("foo AND bar").unwrap(),
            Query::And(
                Box::new(Query::Word("foo".to_string())),
                Box::new(Query::Word("bar".to_string()))
            )
        );

        assert_eq!(
            parse("foo AND bar AND baz").unwrap(),
            Query::And(
                Box::new(Query::Word("foo".to_string())),
                Box::new(Query::And(
                    Box::new(Query::Word("bar".to_string())),
                    Box::new(Query::Word("baz".to_string()))
                ))
            )
        );
    }

    #[test]
    fn test_paren() {
        assert_eq!(parse("(foo)").unwrap(), Query::Word("foo".to_string()));

        assert_eq!(
            parse("(foo AND bar)").unwrap(),
            Query::And(
                Box::new(Query::Word("foo".to_string())),
                Box::new(Query::Word("bar".to_string()))
            )
        );

        assert_eq!(
            parse("(foo AND bar) OR baz").unwrap(),
            Query::Or(
                Box::new(Query::And(
                    Box::new(Query::Word("foo".to_string())),
                    Box::new(Query::Word("bar".to_string()))
                )),
                Box::new(Query::Word("baz".to_string()))
            )
        );

        assert_eq!(
            parse("foo AND (bar OR baz)").unwrap(),
            Query::And(
                Box::new(Query::Word("foo".to_string())),
                Box::new(Query::Or(
                    Box::new(Query::Word("bar".to_string())),
                    Box::new(Query::Word("baz".to_string()))
                ))
            )
        );
    }

    #[test]
    fn test_precedence() {
        assert_eq!(
            parse("foo AND bar OR baz").unwrap(),
            Query::And(
                Box::new(Query::Word("foo".to_string())),
                Box::new(Query::Or(
                    Box::new(Query::Word("bar".to_string())),
                    Box::new(Query::Word("baz".to_string())),
                ))
            )
        );

        assert_eq!(
            parse("foo OR bar AND baz").unwrap(),
            Query::And(
                Box::new(Query::Or(
                    Box::new(Query::Word("foo".to_string())),
                    Box::new(Query::Word("bar".to_string())),
                )),
                Box::new(Query::Word("baz".to_string())),
            )
        );
    }
}
