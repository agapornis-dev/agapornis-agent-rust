use super::{replacement_for_current, scalar_text};
use anyhow::{Context, Result};
use serde_json::{Map, Value};
use std::io::Cursor;
use xmltree::{Element, EmitterConfig, XMLNode};

pub(super) fn apply_xml_parser(bytes: &[u8], find: &Map<String, Value>) -> Result<Vec<u8>> {
    let mut root = Element::parse(Cursor::new(bytes)).context("parse XML configuration")?;

    for (path, replacement) in find {
        let parts = path
            .split('.')
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>();
        if parts.is_empty() {
            continue;
        }

        let start = usize::from(parts[0] == root.name);
        set_xml_path(&mut root, &parts[start..], replacement);
    }

    let mut output = Vec::new();
    root.write_with_config(
        &mut output,
        EmitterConfig::new()
            .perform_indent(true)
            .write_document_declaration(true),
    )?;
    Ok(output)
}

fn set_xml_path(element: &mut Element, path: &[&str], replacement: &Value) {
    let Some((part, rest)) = path.split_first() else {
        set_xml_value(element, replacement);
        return;
    };

    if *part == "*" {
        for child in element.children.iter_mut().filter_map(|node| match node {
            XMLNode::Element(child) => Some(child),
            _ => None,
        }) {
            set_xml_path(child, rest, replacement);
        }
        return;
    }

    let indices = element
        .children
        .iter()
        .enumerate()
        .filter_map(|(index, node)| match node {
            XMLNode::Element(child) if child.name == *part => Some(index),
            _ => None,
        })
        .collect::<Vec<_>>();

    if indices.is_empty() {
        element.children.push(XMLNode::Element(Element::new(part)));
        let index = element.children.len() - 1;
        if let XMLNode::Element(child) = &mut element.children[index] {
            set_xml_path(child, rest, replacement);
        }
    } else {
        for index in indices {
            if let XMLNode::Element(child) = &mut element.children[index] {
                set_xml_path(child, rest, replacement);
            }
        }
    }
}

fn set_xml_value(element: &mut Element, replacement: &Value) {
    let current = element
        .get_text()
        .map(|value| value.into_owned())
        .unwrap_or_default();
    let Some(value) = replacement_for_current(replacement, &current) else {
        return;
    };
    let value = scalar_text(&value);

    if let Some((attribute, value)) = xml_attribute_replacement(&value) {
        element.attributes.insert(attribute, value);
        return;
    }

    element
        .children
        .retain(|node| !matches!(node, XMLNode::Text(_) | XMLNode::CData(_)));
    element.children.push(XMLNode::Text(value));
}

fn xml_attribute_replacement(value: &str) -> Option<(String, String)> {
    let inner = value.strip_prefix('[')?.strip_suffix(']')?;
    let (key, value) = inner.split_once('=')?;
    let value = value.strip_prefix('\'')?.strip_suffix('\'')?;
    Some((key.to_owned(), value.to_owned()))
}
