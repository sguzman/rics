use crate::config::{DateConfig, ExtractFormat, FieldRule, LoadedSource, SourceConfig};
use crate::fetch::FetchedDocument;
use crate::model::{CandidateEvent, EventTimeSpec};
use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, TimeZone, Utc};
use chrono_tz::Tz;
use regex::Regex;
use reqwest::blocking::Client;
use scraper::{ElementRef, Html, Selector};
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};
use tracing::{debug, info, warn};
use url::Url;

pub trait CustomParser: Send + Sync {
    fn key(&self) -> &'static str;
    fn parse(&self, source: &LoadedSource, docs: &[FetchedDocument])
    -> Result<Vec<CandidateEvent>>;
}

pub fn parse_source_events(
    source: &LoadedSource,
    docs: &[FetchedDocument],
) -> Result<Vec<CandidateEvent>> {
    if let Some(parser_key) = source
        .config
        .custom
        .parser
        .as_ref()
        .filter(|_| source.config.custom.enabled)
    {
        if let Some(result) = run_custom_parser(parser_key, source, docs) {
            let events = result?;
            info!(
                source = %source.config.source.key,
                parser = %parser_key,
                events = events.len(),
                "custom parser produced events"
            );
            return Ok(events);
        }
        warn!(
            source = %source.config.source.key,
            parser = %parser_key,
            "custom parser not found; falling back to declarative parser"
        );
    }

    parse_declarative_events(source, docs)
}

fn run_custom_parser(
    parser_key: &str,
    source: &LoadedSource,
    docs: &[FetchedDocument],
) -> Option<Result<Vec<CandidateEvent>>> {
    let parser: Box<dyn CustomParser> = match parser_key {
        "oecd_publications_v1" => Box::new(OecdPublicationsParser),
        "rough_text_lines_v1" => Box::new(RoughTextLinesParser),
        "econ_indicators_calendar_v1" => Box::new(EconIndicatorsCalendarParser),
        _ => return None,
    };
    Some(parser.parse(source, docs))
}

fn parse_declarative_events(
    source: &LoadedSource,
    docs: &[FetchedDocument],
) -> Result<Vec<CandidateEvent>> {
    let mut mapped_records = Vec::new();

    for doc in docs {
        let records = match source.config.extract.format {
            ExtractFormat::Html => parse_html_document(&source.config, doc)?,
            ExtractFormat::Json => parse_json_document(&source.config, doc)?,
            ExtractFormat::PdfText => parse_text_document(&source.config, doc, true)?,
            ExtractFormat::Text => parse_text_document(&source.config, doc, false)?,
        };
        mapped_records.extend(records);
    }

    let mut events = Vec::new();
    for mapped in mapped_records {
        if let Some(event) = mapped_record_to_event(&source.config, mapped)? {
            events.push(event);
        }
    }

    Ok(events)
}

#[derive(Debug, Clone)]
struct MappedRecord {
    fields: BTreeMap<String, String>,
    source_url: String,
    base_url: Option<String>,
    raw_text: String,
}

#[derive(Clone, Copy)]
enum MappingCtx<'a> {
    Html { node: ElementRef<'a>, doc: &'a Html },
    Json { value: &'a Value },
    Text,
}

fn parse_html_document(source: &SourceConfig, doc: &FetchedDocument) -> Result<Vec<MappedRecord>> {
    let html_text = String::from_utf8_lossy(&doc.body).to_string();
    let parsed = Html::parse_document(&html_text);

    let base_url = Url::parse(&doc.source_url)
        .ok()
        .map(|u| {
            let mut x = u;
            x.set_query(None);
            x.set_fragment(None);
            x.to_string()
        })
        .or_else(|| source.configured_base_url());

    let nodes: Vec<ElementRef<'_>> = if let Some(selector) = source.extract.root_selector.as_ref() {
        let selector = Selector::parse(selector)
            .map_err(|err| anyhow!("invalid root_selector {selector}: {err:?}"))?;
        parsed.select(&selector).collect()
    } else {
        let selector =
            Selector::parse("body").map_err(|_| anyhow!("failed to parse body selector"))?;
        parsed.select(&selector).collect()
    };

    if nodes.is_empty() {
        warn!(source = %source.source.key, url = %doc.source_url, "no html nodes matched; skipping document");
        return Ok(Vec::new());
    }

    let mut out = Vec::new();
    for node in nodes {
        let raw_text = node.text().collect::<Vec<_>>().join(" ");
        let mut mapped = BTreeMap::new();

        if source.map.is_empty() {
            if let Some(title) = first_html_text(&node, &["h1", "h2", "h3", "a"]) {
                mapped.insert("title".to_string(), title);
            }
            if let Some(url) = first_html_attr(&node, "a", "href") {
                mapped.insert("url".to_string(), absolutize_url(base_url.as_deref(), &url));
            }
            if let Some(date) = detect_date_in_text(&raw_text) {
                mapped.insert("date".to_string(), date);
            }
        } else {
            for (field, rule) in &source.map {
                let value = evaluate_field_rule(
                    field,
                    rule,
                    MappingCtx::Html { node, doc: &parsed },
                    &mapped,
                    &raw_text,
                    base_url.as_deref(),
                    &doc.source_url,
                )?;

                if let Some(value) = value {
                    mapped.insert(field.clone(), value);
                } else if !rule.optional {
                    debug!(
                        source = %source.source.key,
                        field,
                        "missing non-optional field in html record"
                    );
                }
            }
        }

        out.push(MappedRecord {
            fields: mapped,
            source_url: doc.source_url.clone(),
            base_url: base_url.clone(),
            raw_text,
        });
    }

    Ok(out)
}

fn parse_json_document(source: &SourceConfig, doc: &FetchedDocument) -> Result<Vec<MappedRecord>> {
    let payload: Value = serde_json::from_slice(&doc.body)
        .with_context(|| format!("failed to parse json from {}", doc.source_url))?;
    let nodes = select_json_nodes(&payload, source.extract.root_jsonpath.as_deref());

    let mut out = Vec::new();
    for node in nodes {
        let raw_text = node.to_string();
        let mut mapped = BTreeMap::new();

        if source.map.is_empty() {
            if let Some(obj) = node.as_object() {
                for (k, v) in obj {
                    if let Some(text) = json_value_to_string(v) {
                        mapped.insert(k.clone(), text);
                    }
                }
            }
        } else {
            for (field, rule) in &source.map {
                let value = evaluate_field_rule(
                    field,
                    rule,
                    MappingCtx::Json { value: node },
                    &mapped,
                    &raw_text,
                    None,
                    &doc.source_url,
                )?;
                if let Some(value) = value {
                    mapped.insert(field.clone(), value);
                }
            }
        }

        out.push(MappedRecord {
            fields: mapped,
            source_url: doc.source_url.clone(),
            base_url: None,
            raw_text,
        });
    }

    Ok(out)
}

fn parse_text_document(
    source: &SourceConfig,
    doc: &FetchedDocument,
    from_pdf: bool,
) -> Result<Vec<MappedRecord>> {
    let raw_text = if from_pdf {
        match pdf_extract::extract_text_from_mem(&doc.body) {
            Ok(text) => text,
            Err(err) => {
                warn!(
                    source = %source.source.key,
                    error = %err,
                    "pdf text extraction failed; falling back to utf8 decode"
                );
                String::from_utf8_lossy(&doc.body).to_string()
            }
        }
    } else {
        String::from_utf8_lossy(&doc.body).to_string()
    };

    let processed = normalize_text(
        &raw_text,
        source.pdf.normalize_whitespace,
        source.pdf.join_lines,
    );
    let chunks = split_text_records(source, &processed)?;

    let mut out = Vec::new();
    for chunk in chunks {
        if chunk.trim().is_empty() {
            continue;
        }

        let mut mapped = BTreeMap::new();

        if source.map.is_empty() {
            if let Some(parsed_line) = parse_pipe_record(&chunk) {
                mapped.extend(parsed_line);
            }
        } else {
            for (field, rule) in &source.map {
                let value = evaluate_field_rule(
                    field,
                    rule,
                    MappingCtx::Text,
                    &mapped,
                    &chunk,
                    None,
                    &doc.source_url,
                )?;
                if let Some(value) = value {
                    mapped.insert(field.clone(), value);
                }
            }
        }

        for (field, rule) in &source.pdf.fields {
            if mapped.contains_key(field) {
                continue;
            }
            if let Some(extracted) = extract_with_regex(&chunk, &rule.pattern, rule.capture)? {
                mapped.insert(field.clone(), extracted);
            } else if !rule.optional {
                debug!(
                    source = %source.source.key,
                    field,
                    "missing non-optional pdf field"
                );
            }
        }

        out.push(MappedRecord {
            fields: mapped,
            source_url: doc.source_url.clone(),
            base_url: None,
            raw_text: chunk,
        });
    }

    Ok(out)
}

fn normalize_text(text: &str, normalize_ws: bool, join_lines: bool) -> String {
    let mut working = text.replace("\r\n", "\n");
    if normalize_ws {
        let re = Regex::new(r"[ \t]+").expect("normalize whitespace regex must be valid");
        working = re.replace_all(&working, " ").to_string();
    }
    if join_lines {
        working = working
            .lines()
            .map(str::trim)
            .collect::<Vec<_>>()
            .join("\n");
    }
    working
}

fn split_text_records(source: &SourceConfig, text: &str) -> Result<Vec<String>> {
    if let Some(regex) = source.extract.record_regex.as_ref() {
        let re = Regex::new(regex).with_context(|| format!("invalid record_regex {regex}"))?;
        let mut rows = Vec::new();
        for caps in re.captures_iter(text) {
            if let Some(m) = caps.get(1).or_else(|| caps.get(0)) {
                rows.push(m.as_str().trim().to_string());
            }
        }
        if !rows.is_empty() {
            return Ok(rows);
        }
    }

    if let Some(split) = source.pdf.record_split.first() {
        if split.strategy.eq_ignore_ascii_case("regex") {
            let re = Regex::new(&split.pattern)
                .with_context(|| format!("invalid pdf.record_split pattern {}", split.pattern))?;
            let starts: Vec<usize> = re.find_iter(text).map(|m| m.start()).collect();
            if starts.len() > 1 {
                let mut rows = Vec::new();
                for (idx, start) in starts.iter().enumerate() {
                    let end = if idx + 1 < starts.len() {
                        starts[idx + 1]
                    } else {
                        text.len()
                    };
                    rows.push(text[*start..end].trim().to_string());
                }
                return Ok(rows);
            }
        }
    }

    let split_double_newline = text
        .split("\n\n")
        .map(str::trim)
        .filter(|x| !x.is_empty())
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    if split_double_newline.len() > 1 {
        return Ok(split_double_newline);
    }

    Ok(text
        .lines()
        .map(str::trim)
        .filter(|x| !x.is_empty())
        .map(ToString::to_string)
        .collect())
}

fn parse_pipe_record(line: &str) -> Option<BTreeMap<String, String>> {
    let parts = line
        .split('|')
        .map(str::trim)
        .filter(|x| !x.is_empty())
        .collect::<Vec<_>>();

    if parts.len() < 2 {
        return None;
    }

    let mut map = BTreeMap::new();
    map.insert("date".to_string(), parts[0].to_string());
    map.insert("title".to_string(), parts[1].to_string());
    if parts.len() > 2 {
        map.insert("url".to_string(), parts[2].to_string());
    }
    Some(map)
}

fn evaluate_field_rule(
    field_name: &str,
    rule: &FieldRule,
    ctx: MappingCtx<'_>,
    existing: &BTreeMap<String, String>,
    raw_text: &str,
    base_url: Option<&str>,
    source_url: &str,
) -> Result<Option<String>> {
    let mut value = if let Some(const_value) = &rule.const_value {
        Some(const_value.clone())
    } else {
        let from = rule.from.as_deref().unwrap_or(field_name);
        evaluate_from_expression(from, ctx, existing, raw_text, source_url)?
    };

    if let Some(pattern) = &rule.regex {
        if let Some(v) = value.take() {
            value = extract_with_regex(&v, pattern, rule.capture.unwrap_or(1))?;
        }
    }

    if rule.trim {
        value = value.map(|v| v.trim().to_string());
    }

    if rule.absolutize {
        value = value.map(|v| absolutize_url(base_url, &v));
    }

    if value.as_ref().is_some_and(|v| v.is_empty()) {
        return Ok(None);
    }

    Ok(value)
}

fn evaluate_from_expression(
    expr: &str,
    ctx: MappingCtx<'_>,
    existing: &BTreeMap<String, String>,
    raw_text: &str,
    source_url: &str,
) -> Result<Option<String>> {
    if let Some(key) = expr.strip_prefix("field:") {
        return Ok(existing.get(key).cloned());
    }
    if expr == "source_url" {
        return Ok(Some(source_url.to_string()));
    }
    if let Some(pattern) = expr.strip_prefix("regex:") {
        return extract_with_regex(raw_text, pattern, 1);
    }

    match ctx {
        MappingCtx::Html { node, doc } => {
            if let Some(css) = expr.strip_prefix("css:") {
                return Ok(extract_css_value(node, doc, css));
            }
        }
        MappingCtx::Json { value } => {
            if let Some(path) = expr.strip_prefix("json:") {
                let selected = select_json_value(value, path);
                return Ok(selected.as_ref().and_then(json_value_to_string));
            }
        }
        MappingCtx::Text => {}
    }

    Ok(existing.get(expr).cloned())
}

fn extract_css_value(node: ElementRef<'_>, doc: &Html, expression: &str) -> Option<String> {
    let (selector_text, attr) = split_selector_attr(expression);
    let selector = Selector::parse(selector_text).ok()?;

    if let Some(el) = node.select(&selector).next() {
        return Some(element_attr_or_text(el, attr));
    }

    doc.select(&selector)
        .next()
        .map(|el| element_attr_or_text(el, attr))
}

fn split_selector_attr(expression: &str) -> (&str, Option<&str>) {
    if let Some((selector, attr)) = expression.rsplit_once('@') {
        if !attr.is_empty() && !attr.contains(' ') {
            return (selector, Some(attr));
        }
    }
    (expression, None)
}

fn element_attr_or_text(element: ElementRef<'_>, attr: Option<&str>) -> String {
    if let Some(attr) = attr {
        return element
            .value()
            .attr(attr)
            .map(ToString::to_string)
            .unwrap_or_default();
    }

    element
        .text()
        .collect::<Vec<_>>()
        .join(" ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn first_html_text(node: &ElementRef<'_>, selectors: &[&str]) -> Option<String> {
    for selector in selectors {
        let parsed = Selector::parse(selector).ok()?;
        if let Some(el) = node.select(&parsed).next() {
            let text = element_attr_or_text(el, None);
            if !text.is_empty() {
                return Some(text);
            }
        }
    }
    None
}

fn first_html_attr(node: &ElementRef<'_>, selector: &str, attr: &str) -> Option<String> {
    let parsed = Selector::parse(selector).ok()?;
    node.select(&parsed)
        .next()
        .and_then(|el| el.value().attr(attr).map(ToString::to_string))
}

fn select_json_nodes<'a>(root: &'a Value, path: Option<&str>) -> Vec<&'a Value> {
    match path {
        None => match root {
            Value::Array(items) => items.iter().collect(),
            _ => vec![root],
        },
        Some(path) => {
            if path.trim().is_empty() {
                return vec![root];
            }

            if path == "$" {
                return vec![root];
            }

            if let Some(pointer_path) = path.strip_prefix('/') {
                let pointer = format!("/{pointer_path}");
                return root
                    .pointer(&pointer)
                    .map(|v| match v {
                        Value::Array(items) => items.iter().collect(),
                        _ => vec![v],
                    })
                    .unwrap_or_default();
            }

            if let Some(tokens) = jsonpath_tokens(path) {
                let mut current = vec![root];
                for token in tokens {
                    let mut next = Vec::new();
                    match token {
                        JsonToken::Key(key) => {
                            for value in current {
                                if let Some(found) = value.get(key) {
                                    next.push(found);
                                }
                            }
                        }
                        JsonToken::All(key) => {
                            for value in current {
                                if let Some(Value::Array(items)) = value.get(key) {
                                    next.extend(items.iter());
                                }
                            }
                        }
                        JsonToken::Index(key, idx) => {
                            for value in current {
                                if let Some(Value::Array(items)) = value.get(key)
                                    && let Some(found) = items.get(idx)
                                {
                                    next.push(found);
                                }
                            }
                        }
                    }
                    current = next;
                    if current.is_empty() {
                        break;
                    }
                }

                if current.is_empty() {
                    vec![root]
                } else {
                    current
                }
            } else {
                vec![root]
            }
        }
    }
}

fn select_json_value(root: &Value, path: &str) -> Option<Value> {
    let nodes = select_json_nodes(root, Some(path));
    if nodes.is_empty() {
        None
    } else if nodes.len() == 1 {
        Some(nodes[0].clone())
    } else {
        Some(Value::Array(nodes.into_iter().cloned().collect()))
    }
}

#[derive(Debug)]
enum JsonToken<'a> {
    Key(&'a str),
    All(&'a str),
    Index(&'a str, usize),
}

fn jsonpath_tokens(path: &str) -> Option<Vec<JsonToken<'_>>> {
    let trimmed = path.trim();
    let stripped = trimmed.strip_prefix("$.")?;
    let mut tokens = Vec::new();
    for part in stripped.split('.') {
        if let Some(key) = part.strip_suffix("[*]") {
            tokens.push(JsonToken::All(key));
            continue;
        }
        if let Some((key, idx_part)) = part.split_once('[')
            && let Some(idx_str) = idx_part.strip_suffix(']')
            && let Ok(idx) = idx_str.parse::<usize>()
        {
            tokens.push(JsonToken::Index(key, idx));
            continue;
        }
        tokens.push(JsonToken::Key(part));
    }
    Some(tokens)
}

fn json_value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(s) => Some(s.to_string()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(v) => Some(v.to_string()),
        other => Some(other.to_string()),
    }
}

fn mapped_record_to_event(
    source: &SourceConfig,
    mapped: MappedRecord,
) -> Result<Option<CandidateEvent>> {
    let title = mapped
        .fields
        .get("title")
        .cloned()
        .or_else(|| mapped.fields.get("name").cloned());

    let Some(title) = title else {
        debug!(
            source = %source.source.key,
            raw = %mapped.raw_text,
            "skipping record with no title"
        );
        return Ok(None);
    };

    let source_url = mapped
        .fields
        .get("url")
        .cloned()
        .or_else(|| mapped.fields.get("link").cloned())
        .or_else(|| Some(mapped.source_url.clone()));

    let source_event_id = mapped
        .fields
        .get("source_event_id")
        .cloned()
        .or_else(|| mapped.fields.get("id").cloned());

    let primary_date_key = source.date.primary.as_str();
    let start_raw = mapped
        .fields
        .get("start")
        .cloned()
        .or_else(|| mapped.fields.get(primary_date_key).cloned())
        .or_else(|| mapped.fields.get("date").cloned());

    let end_raw = mapped.fields.get("end").cloned();

    let time = if let Some(start_raw) = start_raw {
        parse_event_time(
            &start_raw,
            end_raw.as_deref(),
            &source.date,
            source
                .source
                .timezone
                .as_deref()
                .or(source.date.assume_timezone.as_deref()),
        )?
    } else {
        EventTimeSpec::Tbd {
            note: mapped.fields.get("tbd").cloned(),
        }
    };

    let status = mapped
        .fields
        .get("status")
        .cloned()
        .unwrap_or_else(|| source.event.status.clone());

    let event_type = mapped
        .fields
        .get("event_type")
        .cloned()
        .unwrap_or_else(|| source.event.event_type.clone());

    let subtype = mapped
        .fields
        .get("subtype")
        .cloned()
        .or_else(|| source.event.subtype.clone());

    let mut categories: HashSet<String> = source.event.categories.iter().cloned().collect();
    categories.insert(source.source.domain.clone());
    if let Some(dynamic) = mapped.fields.get("categories") {
        for item in dynamic.split([',', ';']) {
            let v = item.trim();
            if !v.is_empty() {
                categories.insert(v.to_string());
            }
        }
    }

    let description = mapped
        .fields
        .get("description")
        .cloned()
        .or_else(|| mapped.fields.get("summary").cloned());

    let importance = mapped
        .fields
        .get("importance")
        .and_then(|v| v.parse::<u8>().ok())
        .or(source.event.importance);

    let confidence = mapped
        .fields
        .get("confidence")
        .and_then(|v| v.parse::<f32>().ok());

    let mut metadata = BTreeMap::new();
    for (k, v) in &mapped.fields {
        if [
            "title",
            "name",
            "description",
            "summary",
            "date",
            "start",
            "end",
            "status",
            "event_type",
            "subtype",
            "categories",
            "source_event_id",
            "id",
            "url",
            "link",
            "importance",
            "confidence",
        ]
        .contains(&k.as_str())
        {
            continue;
        }
        metadata.insert(k.clone(), v.clone());
    }
    metadata.insert("time_precision".to_string(), time.precision().to_string());
    if let Some(base_url) = mapped.base_url {
        metadata.insert("base_url".to_string(), base_url);
    }

    Ok(Some(CandidateEvent {
        source_key: source.source.key.clone(),
        source_name: source.source.name.clone(),
        source_event_id,
        source_url,
        title,
        description,
        time,
        timezone: source.source.timezone.clone(),
        status,
        event_type,
        subtype,
        categories: categories.into_iter().collect(),
        jurisdiction: source.source.jurisdiction.clone(),
        country: source.source.default_country.clone(),
        importance,
        confidence,
        metadata,
    }))
}

fn parse_event_time(
    start_raw: &str,
    end_raw: Option<&str>,
    date_cfg: &DateConfig,
    timezone: Option<&str>,
) -> Result<EventTimeSpec> {
    let start_raw = start_raw.trim();
    if start_raw.is_empty() {
        return Ok(EventTimeSpec::Tbd { note: None });
    }

    if let Ok(dt) = DateTime::parse_from_rfc3339(start_raw) {
        let end = end_raw
            .and_then(|s| DateTime::parse_from_rfc3339(s.trim()).ok())
            .map(|d| d.with_timezone(&Utc));
        return Ok(EventTimeSpec::DateTime {
            start: dt.with_timezone(&Utc),
            end,
        });
    }

    for format in &date_cfg.formats {
        if let Ok(dt) = NaiveDateTime::parse_from_str(start_raw, format) {
            let start = localize_datetime(dt, timezone)?;
            let end = end_raw
                .and_then(|raw| NaiveDateTime::parse_from_str(raw.trim(), format).ok())
                .map(|value| localize_datetime(value, timezone))
                .transpose()?;
            return Ok(EventTimeSpec::DateTime { start, end });
        }

        if let Ok(date) = NaiveDate::parse_from_str(start_raw, format) {
            let end = end_raw.and_then(|raw| NaiveDate::parse_from_str(raw.trim(), format).ok());
            return Ok(EventTimeSpec::Date { start: date, end });
        }
    }

    if let Some((month, year)) = parse_month_year(start_raw)
        && date_cfg.allow_month_only
    {
        return Ok(EventTimeSpec::Month { year, month });
    }

    if let Some((quarter, year)) = parse_quarter_year(start_raw) {
        return Ok(EventTimeSpec::Quarter { year, quarter });
    }

    if let Ok(year) = start_raw.parse::<i32>()
        && date_cfg.allow_year_only
    {
        return Ok(EventTimeSpec::Year { year });
    }

    Ok(EventTimeSpec::Tbd {
        note: Some(start_raw.to_string()),
    })
}

fn localize_datetime(value: NaiveDateTime, timezone: Option<&str>) -> Result<DateTime<Utc>> {
    if let Some(tz_name) = timezone
        && let Ok(tz) = tz_name.parse::<Tz>()
    {
        if let Some(dt) = tz
            .from_local_datetime(&value)
            .earliest()
            .or_else(|| tz.from_local_datetime(&value).latest())
        {
            return Ok(dt.with_timezone(&Utc));
        }
    }

    Ok(Utc.from_utc_datetime(&value))
}

fn parse_month_year(value: &str) -> Option<(u32, i32)> {
    for format in ["%B %Y", "%b %Y", "%Y-%m", "%Y/%m"] {
        if let Ok(date) = NaiveDate::parse_from_str(&format!("{value}-01"), &format!("{format}-%d"))
        {
            return Some((date.month(), date.year()));
        }
    }
    None
}

fn parse_quarter_year(value: &str) -> Option<(u8, i32)> {
    let re = Regex::new(r"(?i)^Q([1-4])\s*[- ]?\s*(\d{4})$").ok()?;
    let caps = re.captures(value.trim())?;
    let q = caps.get(1)?.as_str().parse::<u8>().ok()?;
    let year = caps.get(2)?.as_str().parse::<i32>().ok()?;
    Some((q, year))
}

fn detect_date_in_text(text: &str) -> Option<String> {
    let patterns = [
        r"\b\d{4}-\d{2}-\d{2}\b",
        r"\b\d{4}/\d{2}/\d{2}\b",
        r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s+\d{1,2},\s+\d{4}\b",
    ];

    for pat in patterns {
        let regex = Regex::new(pat).ok()?;
        if let Some(found) = regex.find(text) {
            return Some(found.as_str().to_string());
        }
    }

    None
}

fn extract_with_regex(input: &str, pattern: &str, capture: usize) -> Result<Option<String>> {
    let regex = Regex::new(pattern).with_context(|| format!("invalid regex pattern {pattern}"))?;
    let Some(caps) = regex.captures(input) else {
        return Ok(None);
    };
    let Some(value) = caps.get(capture) else {
        return Ok(None);
    };
    Ok(Some(value.as_str().trim().to_string()))
}

fn absolutize_url(base_url: Option<&str>, value: &str) -> String {
    if value.starts_with("http://") || value.starts_with("https://") {
        return value.to_string();
    }

    if let Some(base) = base_url
        && let Ok(base_url) = Url::parse(base)
        && let Ok(joined) = base_url.join(value)
    {
        return joined.to_string();
    }

    value.to_string()
}

struct OecdPublicationsParser;

impl CustomParser for OecdPublicationsParser {
    fn key(&self) -> &'static str {
        "oecd_publications_v1"
    }

    fn parse(
        &self,
        source: &LoadedSource,
        docs: &[FetchedDocument],
    ) -> Result<Vec<CandidateEvent>> {
        if docs.is_empty() {
            return Ok(Vec::new());
        }

        let mut events = Vec::new();
        let current_year = Utc::now().year();
        let mut seen_ids = HashSet::new();
        let first_doc_url = Url::parse(&docs[0].source_url)
            .with_context(|| format!("invalid source url {}", docs[0].source_url))?;
        let mut query_pairs: BTreeMap<String, String> = first_doc_url
            .query_pairs()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        let facet_tags = query_pairs.get("facetTags").cloned().unwrap_or_else(|| {
            "oecd-languages:en,oecd-search-config-pillars:publications".to_string()
        });
        query_pairs.insert(
            "facetTags".to_string(),
            ensure_facet_tags(&facet_tags).to_string(),
        );

        let client = Client::builder()
            .user_agent(
                source
                    .config
                    .fetch
                    .user_agent
                    .clone()
                    .unwrap_or_else(|| "rics/0.1 (+https://example.invalid)".to_string()),
            )
            .build()
            .context("failed to build OECD API client")?;

        let page_size = source
            .config
            .fetch
            .headers
            .get("x-oecd-page-size")
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(50);
        let max_pages = 200usize;
        let mut total = usize::MAX;
        let mut page = 0usize;

        while page < max_pages && page * page_size < total {
            let mut params = query_pairs.clone();
            params.insert("siteName".to_string(), "oecd".to_string());
            params.insert("page".to_string(), page.to_string());
            params.insert("pageSize".to_string(), page_size.to_string());
            params
                .entry("orderBy".to_string())
                .or_insert_with(|| "mostRecent".to_string());
            params
                .entry("minPublicationYear".to_string())
                .or_insert_with(|| current_year.to_string());
            params
                .entry("maxPublicationYear".to_string())
                .or_insert_with(|| current_year.to_string());

            let response = client
                .get("https://api.oecd.org/webcms/search/faceted-search")
                .query(&params)
                .send()
                .with_context(|| format!("failed to query OECD API page {page}"))?;
            if !response.status().is_success() {
                return Err(anyhow!(
                    "OECD API returned {} for page {}",
                    response.status(),
                    page
                ));
            }
            let payload = response
                .json::<Value>()
                .context("failed to decode OECD API JSON")?;

            total = payload.get("total").and_then(|v| v.as_u64()).unwrap_or(0) as usize;

            let Some(results) = payload.get("results").and_then(|v| v.as_array()) else {
                break;
            };

            for result in results {
                let tag_ids = result
                    .get("tags")
                    .and_then(Value::as_array)
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|tag| tag.get("id").and_then(Value::as_str))
                            .map(ToString::to_string)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                let is_publication = tag_ids
                    .iter()
                    .any(|id| id.starts_with("oecd-content-types:publications/"));
                if !is_publication {
                    continue;
                }

                let title = result
                    .get("title")
                    .and_then(|v| v.as_str())
                    .map(str::trim)
                    .filter(|v| !v.is_empty())
                    .map(ToString::to_string);
                let Some(title) = title else {
                    continue;
                };

                let url = result
                    .get("url")
                    .and_then(|v| v.as_str())
                    .map(|v| absolutize_url(Some("https://www.oecd.org"), v))
                    .unwrap_or_default();
                if url.is_empty() {
                    continue;
                }
                if !seen_ids.insert(url.clone()) {
                    continue;
                }

                let date_text = result
                    .get("publicationDateTime")
                    .and_then(|v| v.as_str())
                    .or_else(|| result.get("startDateTime").and_then(|v| v.as_str()))
                    .or_else(|| result.get("endDateTime").and_then(|v| v.as_str()));
                let Some(date_text) = date_text else {
                    continue;
                };

                let time = parse_event_time(
                    date_text,
                    None,
                    &source.config.date,
                    source.config.source.timezone.as_deref(),
                )?;
                if !matches_year_or_next(time.year_bucket(), current_year) {
                    continue;
                }

                let description = result
                    .get("description")
                    .and_then(|v| v.as_str())
                    .map(str::trim)
                    .filter(|v| !v.is_empty())
                    .map(ToString::to_string);

                let tags = tag_ids.join(",");

                events.push(CandidateEvent {
                    source_key: source.config.source.key.clone(),
                    source_name: source.config.source.name.clone(),
                    source_event_id: Some(url.clone()),
                    source_url: Some(url),
                    title,
                    description,
                    time,
                    timezone: source.config.source.timezone.clone(),
                    status: source.config.event.status.clone(),
                    event_type: source.config.event.event_type.clone(),
                    subtype: source.config.event.subtype.clone(),
                    categories: {
                        let mut x = source.config.event.categories.clone();
                        if !x.contains(&"publishing".to_string()) {
                            x.push("publishing".to_string());
                        }
                        x
                    },
                    jurisdiction: source.config.source.jurisdiction.clone(),
                    country: source.config.source.default_country.clone(),
                    importance: source.config.event.importance,
                    confidence: Some(0.95),
                    metadata: BTreeMap::from([
                        ("custom_parser".to_string(), self.key().to_string()),
                        ("api_total".to_string(), total.to_string()),
                        ("api_tags".to_string(), tags),
                    ]),
                });
            }

            page += 1;
        }

        info!(
            source = %source.config.source.key,
            events = events.len(),
            "oecd parser extracted dated publication events"
        );

        Ok(events)
    }
}

fn ensure_facet_tags(tags: &str) -> String {
    let mut values = tags
        .split(',')
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToString::to_string)
        .collect::<Vec<_>>();

    if !values.iter().any(|v| v == "oecd-languages:en") {
        values.push("oecd-languages:en".to_string());
    }
    if !values
        .iter()
        .any(|v| v == "oecd-search-config-pillars:publications")
    {
        values.push("oecd-search-config-pillars:publications".to_string());
    }
    values.join(",")
}

fn matches_year_or_next(year: Option<i32>, current_year: i32) -> bool {
    match year {
        Some(y) => y == current_year || y == current_year + 1,
        None => false,
    }
}

struct RoughTextLinesParser;

impl CustomParser for RoughTextLinesParser {
    fn key(&self) -> &'static str {
        "rough_text_lines_v1"
    }

    fn parse(
        &self,
        source: &LoadedSource,
        docs: &[FetchedDocument],
    ) -> Result<Vec<CandidateEvent>> {
        let mut events = Vec::new();

        for doc in docs {
            let payload = String::from_utf8_lossy(&doc.body);
            for line in payload.lines() {
                let line = line.trim();
                if line.is_empty() || line.starts_with('#') {
                    continue;
                }

                let Some(map) = parse_pipe_record(line) else {
                    continue;
                };

                let Some(title) = map.get("title").cloned() else {
                    continue;
                };

                let time = if let Some(date) = map.get("date") {
                    parse_event_time(
                        date,
                        None,
                        &source.config.date,
                        source.config.source.timezone.as_deref(),
                    )?
                } else {
                    EventTimeSpec::Tbd { note: None }
                };

                events.push(CandidateEvent {
                    source_key: source.config.source.key.clone(),
                    source_name: source.config.source.name.clone(),
                    source_event_id: map.get("url").cloned(),
                    source_url: map.get("url").cloned(),
                    title,
                    description: None,
                    time,
                    timezone: source.config.source.timezone.clone(),
                    status: source.config.event.status.clone(),
                    event_type: source.config.event.event_type.clone(),
                    subtype: source.config.event.subtype.clone(),
                    categories: source.config.event.categories.clone(),
                    jurisdiction: source.config.source.jurisdiction.clone(),
                    country: source.config.source.default_country.clone(),
                    importance: source.config.event.importance,
                    confidence: Some(0.5),
                    metadata: BTreeMap::from([(
                        "custom_parser".to_string(),
                        self.key().to_string(),
                    )]),
                });
            }
        }

        Ok(events)
    }
}

struct EconIndicatorsCalendarParser;

impl CustomParser for EconIndicatorsCalendarParser {
    fn key(&self) -> &'static str {
        "econ_indicators_calendar_v1"
    }

    fn parse(
        &self,
        source: &LoadedSource,
        docs: &[FetchedDocument],
    ) -> Result<Vec<CandidateEvent>> {
        let mut events = Vec::new();
        let day_header = Regex::new(
            r"^(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\s+([A-Za-z]+)\s+(\d{1,2})\s+(\d{4})",
        )
        .expect("day header regex must compile");
        let time_line =
            Regex::new(r"^(\d{1,2}:\d{2}\s*[AP]M)$").expect("time line regex must compile");
        let split_columns = Regex::new(r"\s{2,}").expect("split columns regex must compile");
        let current_country = source
            .config
            .fetch
            .template_vars
            .get("country")
            .cloned()
            .or_else(|| source.config.source.default_country.clone())
            .unwrap_or_else(|| "US".to_string())
            .to_ascii_uppercase();

        for doc in docs {
            let payload = String::from_utf8_lossy(&doc.body);
            let mut active_date: Option<NaiveDate> = None;
            let mut active_time: Option<String> = None;
            let mut waiting_for_country = false;

            for raw in payload.lines() {
                let line = raw.trim();
                if line.is_empty() {
                    continue;
                }

                if let Some(caps) = day_header.captures(line) {
                    let month = caps.get(2).map(|m| m.as_str()).unwrap_or_default();
                    let day = caps.get(3).map(|m| m.as_str()).unwrap_or_default();
                    let year = caps.get(4).map(|m| m.as_str()).unwrap_or_default();
                    let date_str = format!("{month} {day} {year}");
                    active_date = NaiveDate::parse_from_str(&date_str, "%B %d %Y")
                        .ok()
                        .or_else(|| NaiveDate::parse_from_str(&date_str, "%b %d %Y").ok());
                    active_time = None;
                    waiting_for_country = false;
                    continue;
                }

                if let Some(caps) = time_line.captures(line) {
                    active_time = caps.get(1).map(|m| m.as_str().to_string());
                    waiting_for_country = true;
                    continue;
                }

                if waiting_for_country {
                    if line.eq_ignore_ascii_case(&current_country) {
                        waiting_for_country = false;
                        continue;
                    }

                    // If country row is missing, continue with this line as payload.
                    waiting_for_country = false;
                }

                let Some(date) = active_date else {
                    continue;
                };
                let Some(time_text) = active_time.as_deref() else {
                    continue;
                };

                let Some(start) =
                    combine_date_time(date, time_text, source.config.source.timezone.as_deref())?
                else {
                    continue;
                };

                let columns = split_columns
                    .split(line)
                    .map(str::trim)
                    .filter(|v| !v.is_empty())
                    .collect::<Vec<_>>();
                if columns.is_empty() {
                    continue;
                }

                let title = columns[0].to_string();
                let actual = columns.get(1).map(|v| v.to_string());
                let previous = columns.get(2).map(|v| v.to_string());
                let consensus = columns.get(3).map(|v| v.to_string());
                let forecast = columns.get(4).map(|v| v.to_string());

                let mut metadata = BTreeMap::new();
                metadata.insert("country".to_string(), current_country.clone());
                metadata.insert("custom_parser".to_string(), self.key().to_string());
                if let Some(value) = &actual {
                    metadata.insert("actual".to_string(), value.clone());
                }
                if let Some(value) = &previous {
                    metadata.insert("previous".to_string(), value.clone());
                }
                if let Some(value) = &consensus {
                    metadata.insert("consensus".to_string(), value.clone());
                }
                if let Some(value) = &forecast {
                    metadata.insert("forecast".to_string(), value.clone());
                }

                let id = format!(
                    "{}|{}|{}|{}",
                    current_country,
                    date.format("%Y-%m-%d"),
                    time_text,
                    title
                );

                let description = build_econ_description(actual, previous, consensus, forecast);

                events.push(CandidateEvent {
                    source_key: source.config.source.key.clone(),
                    source_name: source.config.source.name.clone(),
                    source_event_id: Some(id),
                    source_url: Some(doc.source_url.clone()),
                    title,
                    description,
                    time: EventTimeSpec::DateTime { start, end: None },
                    timezone: source.config.source.timezone.clone(),
                    status: source.config.event.status.clone(),
                    event_type: source.config.event.event_type.clone(),
                    subtype: source.config.event.subtype.clone(),
                    categories: source.config.event.categories.clone(),
                    jurisdiction: source.config.source.jurisdiction.clone(),
                    country: Some(current_country.clone()),
                    importance: source.config.event.importance,
                    confidence: Some(0.9),
                    metadata,
                });
            }
        }

        Ok(events)
    }
}

fn combine_date_time(
    date: NaiveDate,
    time_text: &str,
    timezone: Option<&str>,
) -> Result<Option<DateTime<Utc>>> {
    let time = NaiveDateTime::parse_from_str(
        &format!("{} {}", date.format("%Y-%m-%d"), time_text.replace(" ", "")),
        "%Y-%m-%d %I:%M%p",
    )
    .ok()
    .or_else(|| {
        NaiveDateTime::parse_from_str(
            &format!("{} {}", date.format("%Y-%m-%d"), time_text),
            "%Y-%m-%d %I:%M %p",
        )
        .ok()
    });

    let Some(naive) = time else {
        return Ok(None);
    };

    if let Some(tz_name) = timezone
        && let Ok(tz) = tz_name.parse::<Tz>()
        && let Some(dt) = tz
            .from_local_datetime(&naive)
            .earliest()
            .or_else(|| tz.from_local_datetime(&naive).latest())
    {
        return Ok(Some(dt.with_timezone(&Utc)));
    }

    Ok(Some(Utc.from_utc_datetime(&naive)))
}

fn build_econ_description(
    actual: Option<String>,
    previous: Option<String>,
    consensus: Option<String>,
    forecast: Option<String>,
) -> Option<String> {
    let mut lines = Vec::new();
    if let Some(v) = actual {
        lines.push(format!("Actual: {v}"));
    }
    if let Some(v) = previous {
        lines.push(format!("Previous: {v}"));
    }
    if let Some(v) = consensus {
        lines.push(format!("Consensus: {v}"));
    }
    if let Some(v) = forecast {
        lines.push(format!("Forecast: {v}"));
    }

    if lines.is_empty() {
        None
    } else {
        Some(lines.join("\n"))
    }
}

trait SourceConfigHelpers {
    fn configured_base_url(&self) -> Option<String>;
}

impl SourceConfigHelpers for SourceConfig {
    fn configured_base_url(&self) -> Option<String> {
        self.fetch.base_url.clone()
    }
}
