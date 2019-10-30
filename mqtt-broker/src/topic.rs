use std::str::FromStr;

use crate::{Error, ErrorKind};

const NUL_CHAR: char = '\0';
const TOPIC_SEPARATOR: char = '/';
static MULTILEVEL_WILDCARD: &str = "#";
static SINGLELEVEL_WILDCARD: &str = "+";

#[derive(Debug, PartialEq)]
pub struct TopicFilter {
    segments: Vec<Segment>,
}

#[derive(Debug, PartialEq)]
enum Segment {
    Level(String),
    SingleLevelWildcard,
    MultiLevelWildcard,
}

impl FromStr for TopicFilter {
    type Err = Error;

    fn from_str(string: &str) -> Result<Self, Self::Err> {
        // [MQTT-4.7.3-1] - All Topic Names and Topic Filters MUST be at least
        // one character long.
        // [MQTT-4.7.3-2] - Topic Names and Topic Filters MUST NOT include the
        // null character (Unicode U+0000).
        if string.is_empty() || string.contains(NUL_CHAR) {
            return Err(Error::from(ErrorKind::InvalidTopicFilter(
                string.to_owned(),
            )));
        }

        let mut segments = Vec::new();
        for s in string.split(TOPIC_SEPARATOR) {
            let segment = if s == MULTILEVEL_WILDCARD {
                Segment::MultiLevelWildcard
            } else if s == SINGLELEVEL_WILDCARD {
                Segment::SingleLevelWildcard
            } else {
                if s.contains(MULTILEVEL_WILDCARD) || s.contains(SINGLELEVEL_WILDCARD) {
                    return Err(Error::from(ErrorKind::InvalidTopicFilter(
                        string.to_owned(),
                    )));
                }
                Segment::Level(s.to_owned())
            };
            segments.push(segment);
        }

        // [MQTT-4.7.1-2] - The multi-level wildcard character MUST be
        // specified either on its own or following a topic level separator.
        // In either case it MUST be the last character specified in the
        // Topic Filter.
        let len = segments.len();
        for (i, segment) in segments.iter().enumerate() {
            if *segment == Segment::MultiLevelWildcard && i != len - 1 {
                return Err(Error::from(ErrorKind::InvalidTopicFilter(
                    string.to_owned(),
                )));
            }
        }

        let filter = TopicFilter { segments };
        Ok(filter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn filter(segments: Vec<Segment>) -> TopicFilter {
        TopicFilter { segments }
    }

    #[test]
    fn topic_filter_valid() {
        let cases = vec![
            (
                "/finance",
                filter(vec![
                    Segment::Level("".to_string()),
                    Segment::Level("finance".to_string()),
                ]),
            ),
            (
                "sport/#",
                filter(vec![
                    Segment::Level("sport".to_string()),
                    Segment::MultiLevelWildcard,
                ]),
            ),
            ("#", filter(vec![Segment::MultiLevelWildcard])),
            (
                "sport/tennis/#",
                filter(vec![
                    Segment::Level("sport".to_string()),
                    Segment::Level("tennis".to_string()),
                    Segment::MultiLevelWildcard,
                ]),
            ),
            ("+", filter(vec![Segment::SingleLevelWildcard])),
            (
                "+/tennis/#",
                filter(vec![
                    Segment::SingleLevelWildcard,
                    Segment::Level("tennis".to_string()),
                    Segment::MultiLevelWildcard,
                ]),
            ),
            (
                "sport/+/player1",
                filter(vec![
                    Segment::Level("sport".to_string()),
                    Segment::SingleLevelWildcard,
                    Segment::Level("player1".to_string()),
                ]),
            ),
        ];

        for (case, expected) in cases.iter() {
            let result = TopicFilter::from_str(case).unwrap();
            assert_eq!(*expected, result);
        }
    }

    #[test]
    fn topic_filter_invalid() {
        let cases = vec![
            "",
            "#/blah",
            "sport/tennis#",
            "sport/tennis/#/ranking",
            "sport+",
            "bla\0h+",
        ];

        for case in cases.iter() {
            let result = TopicFilter::from_str(case);
            assert!(result.is_err());
        }
    }
}
