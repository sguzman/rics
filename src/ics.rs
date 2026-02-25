use crate::config::SourceConfig;
use crate::model::{EventRecord, EventTimeSpec};
use anyhow::{Context, Result};
use chrono::{Datelike, Timelike, Utc};
use std::path::Path;

pub fn write_source_year_calendar(
    source: &SourceConfig,
    year: i32,
    events: &[&EventRecord],
    path: &Path,
) -> Result<()> {
    let mut lines = Vec::new();
    push_line(&mut lines, "BEGIN:VCALENDAR".to_string());
    push_line(&mut lines, "VERSION:2.0".to_string());
    push_line(
        &mut lines,
        "PRODID:-//rics//ICS Generator 1.0//EN".to_string(),
    );
    push_line(&mut lines, "CALSCALE:GREGORIAN".to_string());
    push_line(&mut lines, "METHOD:PUBLISH".to_string());
    push_line(
        &mut lines,
        format!("X-WR-CALNAME:{} {}", escape_text(&source.source.name), year),
    );
    push_line(&mut lines, "X-WR-TIMEZONE:UTC".to_string());

    for event in events {
        append_event_lines(&mut lines, event);
    }

    push_line(&mut lines, "END:VCALENDAR".to_string());

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create output dir {}", parent.display()))?;
    }

    std::fs::write(path, lines.join("\r\n") + "\r\n")
        .with_context(|| format!("failed to write ics {}", path.display()))?;

    Ok(())
}

fn append_event_lines(lines: &mut Vec<String>, event: &EventRecord) {
    push_line(lines, "BEGIN:VEVENT".to_string());
    push_line(lines, format!("UID:{}", escape_text(&event.uid)));
    push_line(
        lines,
        format!("DTSTAMP:{}", format_utc(event.last_modified)),
    );
    push_line(lines, format!("CREATED:{}", format_utc(event.created_at)));
    push_line(
        lines,
        format!("LAST-MODIFIED:{}", format_utc(event.last_modified)),
    );
    push_line(lines, format!("SEQUENCE:{}", event.sequence));

    match &event.time {
        EventTimeSpec::DateTime { start, end } => {
            push_line(lines, format!("DTSTART:{}", format_utc(*start)));
            if let Some(end) = end {
                push_line(lines, format!("DTEND:{}", format_utc(*end)));
            }
        }
        EventTimeSpec::Date { start, end } => {
            push_line(lines, format!("DTSTART;VALUE=DATE:{}", format_date(*start)));
            let exclusive_end = end.unwrap_or(*start).succ_opt().unwrap_or(*start);
            push_line(
                lines,
                format!("DTEND;VALUE=DATE:{}", format_date(exclusive_end)),
            );
        }
        EventTimeSpec::Month { year, month } => {
            if let Some(start) = chrono::NaiveDate::from_ymd_opt(*year, *month, 1) {
                push_line(lines, format!("DTSTART;VALUE=DATE:{}", format_date(start)));
                if let Some(end) = event.time.end_date_exclusive() {
                    push_line(lines, format!("DTEND;VALUE=DATE:{}", format_date(end)));
                }
            }
        }
        EventTimeSpec::Quarter { year, quarter } => {
            let month = 1 + ((*quarter as u32).saturating_sub(1) * 3);
            if let Some(start) = chrono::NaiveDate::from_ymd_opt(*year, month, 1) {
                push_line(lines, format!("DTSTART;VALUE=DATE:{}", format_date(start)));
                if let Some(end) = event.time.end_date_exclusive() {
                    push_line(lines, format!("DTEND;VALUE=DATE:{}", format_date(end)));
                }
            }
        }
        EventTimeSpec::Year { year } => {
            if let Some(start) = chrono::NaiveDate::from_ymd_opt(*year, 1, 1) {
                push_line(lines, format!("DTSTART;VALUE=DATE:{}", format_date(start)));
                if let Some(end) = event.time.end_date_exclusive() {
                    push_line(lines, format!("DTEND;VALUE=DATE:{}", format_date(end)));
                }
            }
        }
        EventTimeSpec::Tbd { note } => {
            if let Some(note) = note {
                push_line(lines, format!("X-RICS-TBD-NOTE:{}", escape_text(note)));
            }
        }
    }

    push_line(lines, format!("SUMMARY:{}", escape_text(&event.title)));

    if let Some(description) = &event.description {
        push_line(lines, format!("DESCRIPTION:{}", escape_text(description)));
    }

    if let Some(url) = &event.source_url {
        push_line(lines, format!("URL:{}", escape_text(url)));
    }

    if !event.categories.is_empty() {
        let mut categories = event
            .categories
            .iter()
            .map(|v| escape_text(v))
            .collect::<Vec<_>>();
        categories.sort();
        categories.dedup();
        push_line(lines, format!("CATEGORIES:{}", categories.join(",")));
    }

    push_line(
        lines,
        format!("STATUS:{}", event.status.to_ascii_uppercase()),
    );
    push_line(lines, "TRANSP:TRANSPARENT".to_string());

    push_line(
        lines,
        format!("X-RICS-SOURCE-KEY:{}", escape_text(&event.source_key)),
    );
    push_line(
        lines,
        format!("X-RICS-EVENT-TYPE:{}", escape_text(&event.event_type)),
    );
    if let Some(subtype) = &event.subtype {
        push_line(
            lines,
            format!("X-RICS-EVENT-SUBTYPE:{}", escape_text(subtype)),
        );
    }
    if let Some(importance) = event.importance {
        push_line(lines, format!("X-RICS-IMPORTANCE:{}", importance));
    }
    if let Some(confidence) = event.confidence {
        push_line(lines, format!("X-RICS-CONFIDENCE:{confidence:.4}"));
    }
    push_line(
        lines,
        format!(
            "X-RICS-TIME-PRECISION:{}",
            event.time.precision().to_ascii_uppercase()
        ),
    );
    push_line(
        lines,
        format!("X-RICS-REVISION-HASH:{}", event.revision_hash),
    );

    for (key, value) in &event.metadata {
        if key.is_empty() || value.is_empty() {
            continue;
        }
        let x_key = format!("X-RICS-{}", sanitize_x_key(key));
        push_line(lines, format!("{x_key}:{}", escape_text(value)));
    }

    push_line(lines, "END:VEVENT".to_string());
}

fn sanitize_x_key(raw: &str) -> String {
    raw.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_uppercase()
            } else {
                '-'
            }
        })
        .collect()
}

fn push_line(lines: &mut Vec<String>, line: String) {
    for folded in fold_line(&line) {
        lines.push(folded);
    }
}

fn fold_line(line: &str) -> Vec<String> {
    const LIMIT: usize = 75;

    if line.len() <= LIMIT {
        return vec![line.to_string()];
    }

    let mut chunks = Vec::new();
    let mut current = String::new();

    for ch in line.chars() {
        let next_len = current.len() + ch.len_utf8();
        if next_len > LIMIT {
            if chunks.is_empty() {
                chunks.push(current.clone());
            } else {
                chunks.push(format!(" {current}"));
            }
            current.clear();
        }
        current.push(ch);
    }

    if !current.is_empty() {
        if chunks.is_empty() {
            chunks.push(current);
        } else {
            chunks.push(format!(" {current}"));
        }
    }

    chunks
}

fn format_utc(value: chrono::DateTime<Utc>) -> String {
    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        value.year(),
        value.month(),
        value.day(),
        value.hour(),
        value.minute(),
        value.second()
    )
}

fn format_date(value: chrono::NaiveDate) -> String {
    format!("{:04}{:02}{:02}", value.year(), value.month(), value.day())
}

fn escape_text(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace(';', "\\;")
        .replace(',', "\\,")
        .replace('\n', "\\n")
}
