use super::{replacement_for_current, scalar_text};
use anyhow::{Context, Result};
use serde_json::{Map, Number, Value};

pub(super) fn apply_json_parser(bytes: &[u8], find: &Map<String, Value>) -> Result<Vec<u8>> {
    let mut document: Value = serde_json::from_slice(bytes).context("parse JSON configuration")?;
    apply_structured_replacements(&mut document, find);
    Ok(serde_json::to_vec_pretty(&document)?)
}

pub(super) fn apply_yaml_parser(bytes: &[u8], find: &Map<String, Value>) -> Result<Vec<u8>> {
    let mut document: Value = serde_yaml::from_slice(bytes).context("parse YAML configuration")?;
    apply_structured_replacements(&mut document, find);
    Ok(serde_yaml::to_string(&document)?.into_bytes())
}

pub(super) fn apply_structured_replacements(document: &mut Value, find: &Map<String, Value>) {
    for (path, replacement) in find {
        let segments = parse_path(path);
        set_structured_path(document, &segments, replacement);
    }
}

#[derive(Debug, PartialEq)]
enum PathSegment {
    Key(String),
    Index(usize),
    Wildcard,
}

fn parse_path(path: &str) -> Vec<PathSegment> {
    let mut output = Vec::new();

    for part in path.split('.').filter(|part| !part.is_empty()) {
        if part == "*" {
            output.push(PathSegment::Wildcard);
            continue;
        }

        let mut rest = part;
        if let Some(index) = rest.find('[') {
            if index > 0 {
                output.push(PathSegment::Key(rest[..index].to_owned()));
            }
            rest = &rest[index..];

            while let Some(tail) = rest.strip_prefix('[') {
                let Some(end) = tail.find(']') else {
                    break;
                };
                if let Ok(index) = tail[..end].parse() {
                    output.push(PathSegment::Index(index));
                }
                rest = &tail[end + 1..];
            }
        } else {
            output.push(PathSegment::Key(rest.to_owned()));
        }
    }

    output
}

fn set_structured_path(node: &mut Value, path: &[PathSegment], replacement: &Value) {
    let Some((segment, rest)) = path.split_first() else {
        if let Some(value) = replacement_for_current(replacement, &scalar_text(node)) {
            *node = structured_value(value);
        }
        return;
    };

    match segment {
        PathSegment::Wildcard => match node {
            Value::Object(object) => {
                for child in object.values_mut() {
                    set_structured_path(child, rest, replacement);
                }
            }
            Value::Array(array) => {
                for child in array {
                    set_structured_path(child, rest, replacement);
                }
            }
            _ => {}
        },
        PathSegment::Key(key) => {
            if !node.is_object() {
                *node = Value::Object(Map::new());
            }
            let child = node
                .as_object_mut()
                .expect("node was converted to object")
                .entry(key.clone())
                .or_insert_with(|| initial_value(rest.first()));
            set_structured_path(child, rest, replacement);
        }
        PathSegment::Index(index) => {
            if !node.is_array() {
                *node = Value::Array(Vec::new());
            }
            let array = node.as_array_mut().expect("node was converted to array");
            while array.len() <= *index {
                array.push(initial_value(rest.first()));
            }
            set_structured_path(&mut array[*index], rest, replacement);
        }
    }
}

fn initial_value(next: Option<&PathSegment>) -> Value {
    match next {
        Some(PathSegment::Index(_)) => Value::Array(Vec::new()),
        _ => Value::Object(Map::new()),
    }
}

fn structured_value(value: Value) -> Value {
    let Value::String(text) = value else {
        return value;
    };

    if let Ok(value) = text.parse::<i64>() {
        return Value::Number(value.into());
    }
    if let Ok(value) = text.parse::<u64>() {
        return Value::Number(value.into());
    }
    if let Ok(value) = text.parse::<f64>()
        && let Some(value) = Number::from_f64(value)
    {
        return Value::Number(value);
    }

    Value::String(text)
}
