use super::{replacement_for_current, scalar_text};
use serde_json::{Map, Value};

pub(super) fn apply_file_parser(bytes: &[u8], find: &Map<String, Value>) -> Vec<u8> {
    let text = String::from_utf8_lossy(bytes).replace("\r\n", "\n");
    let had_trailing_newline = text.ends_with('\n');
    let mut lines = text.lines().map(str::to_owned).collect::<Vec<_>>();

    for (needle, value) in find {
        let replacement = scalar_text(value);
        let mut replaced = false;

        for line in &mut lines {
            if line.starts_with(needle) {
                *line = replacement.clone();
                replaced = true;
            }
        }

        if !replaced {
            lines.push(replacement);
        }
    }

    finish_lines(lines, had_trailing_newline)
}

pub(super) fn apply_properties_parser(bytes: &[u8], find: &Map<String, Value>) -> Vec<u8> {
    let text = String::from_utf8_lossy(bytes).replace("\r\n", "\n");
    let had_trailing_newline = text.ends_with('\n');
    let mut lines = text.lines().map(str::to_owned).collect::<Vec<_>>();

    for (key, replacement) in find {
        let mut replaced = false;

        for line in &mut lines {
            let Some((line_key, value_start)) = properties_key(line) else {
                continue;
            };

            if line_key != key {
                continue;
            }

            let current = line[value_start..].trim();
            if let Some(value) = replacement_for_current(replacement, current) {
                *line = format!("{key}={}", scalar_text(&value));
            }
            replaced = true;
        }

        if !replaced && !replacement.is_object() {
            lines.push(format!("{key}={}", scalar_text(replacement)));
        }
    }

    finish_lines(lines, had_trailing_newline)
}

fn properties_key(line: &str) -> Option<(&str, usize)> {
    let trimmed = line.trim_start();
    if trimmed.is_empty() || trimmed.starts_with('#') || trimmed.starts_with('!') {
        return None;
    }

    let leading = line.len() - trimmed.len();
    let split = trimmed
        .char_indices()
        .find(|(_, character)| matches!(character, '=' | ':') || character.is_whitespace());

    match split {
        Some((index, _)) => {
            let mut value_start = leading + index;
            let tail = &line[value_start..];
            value_start += tail
                .char_indices()
                .take_while(|(_, character)| {
                    character.is_whitespace() || matches!(character, '=' | ':')
                })
                .last()
                .map(|(index, character)| index + character.len_utf8())
                .unwrap_or(0);
            Some((&trimmed[..index], value_start))
        }
        None => Some((trimmed, line.len())),
    }
}

pub(super) fn apply_ini_parser(bytes: &[u8], find: &Map<String, Value>) -> Vec<u8> {
    let text = String::from_utf8_lossy(bytes).replace("\r\n", "\n");
    let had_trailing_newline = text.ends_with('\n');
    let mut lines = text.lines().map(str::to_owned).collect::<Vec<_>>();

    for (path, replacement) in find {
        let (wanted_section, wanted_key) = path
            .split_once('.')
            .map_or(("", path.as_str()), |(section, key)| (section, key));
        let mut section = String::new();
        let mut replaced = false;

        for line in &mut lines {
            let trimmed = line.trim();
            if trimmed.starts_with('[') && trimmed.ends_with(']') {
                section = trimmed[1..trimmed.len() - 1].to_owned();
                continue;
            }

            if section != wanted_section {
                continue;
            }

            let Some((key, current)) = trimmed.split_once(['=', ':']) else {
                continue;
            };

            if key.trim() != wanted_key {
                continue;
            }

            if let Some(value) = replacement_for_current(replacement, current.trim()) {
                *line = format!("{wanted_key}={}", scalar_text(&value));
            }
            replaced = true;
        }

        if !replaced && !replacement.is_object() {
            if wanted_section.is_empty() {
                let insertion = lines
                    .iter()
                    .position(|line| {
                        let line = line.trim();
                        line.starts_with('[') && line.ends_with(']')
                    })
                    .unwrap_or(lines.len());
                lines.insert(
                    insertion,
                    format!("{wanted_key}={}", scalar_text(replacement)),
                );
            } else {
                let header = format!("[{wanted_section}]");
                if let Some(start) = lines.iter().position(|line| line.trim() == header) {
                    let insertion = lines[start + 1..]
                        .iter()
                        .position(|line| {
                            let line = line.trim();
                            line.starts_with('[') && line.ends_with(']')
                        })
                        .map(|offset| start + 1 + offset)
                        .unwrap_or(lines.len());
                    lines.insert(
                        insertion,
                        format!("{wanted_key}={}", scalar_text(replacement)),
                    );
                } else {
                    if !lines.is_empty() && !lines.last().is_some_and(String::is_empty) {
                        lines.push(String::new());
                    }
                    lines.push(header);
                    lines.push(format!("{wanted_key}={}", scalar_text(replacement)));
                }
            }
        }
    }

    finish_lines(lines, had_trailing_newline)
}

fn finish_lines(lines: Vec<String>, trailing_newline: bool) -> Vec<u8> {
    let mut output = lines.join("\n");
    if trailing_newline && !output.ends_with('\n') {
        output.push('\n');
    }
    output.into_bytes()
}
