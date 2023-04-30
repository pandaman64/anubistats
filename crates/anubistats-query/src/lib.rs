#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Query {
    Word(String),
    And(Box<Query>, Box<Query>),
    Or(Box<Query>, Box<Query>),
}

fn primary_expr(input: &str) -> (&str, Query) {
    let input = input.trim_start();
    if let Some(input) = input.strip_prefix('(') {
        let (input, query) = expr(input);
        let input = input.trim_start();
        assert!(input.starts_with(')'));
        (&input[1..], query)
    } else {
        let (word, input) = match input.find(|c: char| !c.is_alphanumeric()) {
            Some(idx) => input.split_at(idx),
            None => (input, ""),
        };
        (input, Query::Word(word.to_string()))
    }
}

fn or_expr(input: &str) -> (&str, Query) {
    let (input, lhs) = primary_expr(input);
    let input = input.trim_start();
    if let Some(input) = input.strip_prefix("OR") {
        let (input, rhs) = or_expr(input);
        (input, Query::Or(Box::new(lhs), Box::new(rhs)))
    } else {
        (input, lhs)
    }
}

fn and_expr(input: &str) -> (&str, Query) {
    let (input, lhs) = or_expr(input);
    let input = input.trim_start();
    if let Some(input) = input.strip_prefix("AND") {
        let (input, rhs) = and_expr(input);
        (input, Query::And(Box::new(lhs), Box::new(rhs)))
    } else {
        (input, lhs)
    }
}

fn expr(input: &str) -> (&str, Query) {
    and_expr(input)
}

pub fn parse(input: &str) -> Query {
    let (input, query) = expr(input);
    assert!(input.trim().is_empty());
    query
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_or() {
        assert_eq!(
            parse("foo OR bar"),
            Query::Or(
                Box::new(Query::Word("foo".to_string())),
                Box::new(Query::Word("bar".to_string()))
            )
        );

        assert_eq!(
            parse("foo OR bar OR baz"),
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
            parse("foo AND bar"),
            Query::And(
                Box::new(Query::Word("foo".to_string())),
                Box::new(Query::Word("bar".to_string()))
            )
        );

        assert_eq!(
            parse("foo AND bar AND baz"),
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
        assert_eq!(parse("(foo)"), Query::Word("foo".to_string()));

        assert_eq!(
            parse("(foo AND bar)"),
            Query::And(
                Box::new(Query::Word("foo".to_string())),
                Box::new(Query::Word("bar".to_string()))
            )
        );

        assert_eq!(
            parse("(foo AND bar) OR baz"),
            Query::Or(
                Box::new(Query::And(
                    Box::new(Query::Word("foo".to_string())),
                    Box::new(Query::Word("bar".to_string()))
                )),
                Box::new(Query::Word("baz".to_string()))
            )
        );

        assert_eq!(
            parse("foo AND (bar OR baz)"),
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
            parse("foo AND bar OR baz"),
            Query::And(
                Box::new(Query::Word("foo".to_string())),
                Box::new(Query::Or(
                    Box::new(Query::Word("bar".to_string())),
                    Box::new(Query::Word("baz".to_string())),
                ))
            )
        );

        assert_eq!(
            parse("foo OR bar AND baz"),
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
