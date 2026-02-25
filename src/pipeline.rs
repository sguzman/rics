use crate::config::{LoadedSource, load_source_file, load_sources_from_dir};
use crate::fetch::fetch_source_documents;
use crate::ics::write_source_year_calendar;
use crate::model::{CandidateEvent, EventRecord, SourceRunReport, State};
use crate::parser::parse_source_events;
use crate::store::{load_state, save_state};
use anyhow::{Context, Result, bail};
use chrono::Utc;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub struct SyncOptions {
    pub config_dir: PathBuf,
    pub state_path: PathBuf,
    pub out_dir: PathBuf,
    pub source: Option<String>,
    pub dry_run: bool,
}

#[derive(Debug, Clone)]
pub struct BuildOptions {
    pub config_dir: PathBuf,
    pub state_path: PathBuf,
    pub out_dir: PathBuf,
    pub source: Option<String>,
    pub year: Option<i32>,
}

#[derive(Debug, Clone)]
pub struct ValidateOptions {
    pub config_dir: Option<PathBuf>,
    pub source_file: Option<PathBuf>,
}

pub fn sync_sources(options: &SyncOptions) -> Result<Vec<SourceRunReport>> {
    let mut sources = load_sources_from_dir(&options.config_dir)?;
    if let Some(filter) = &options.source {
        sources.retain(|s| s.config.source.key == *filter);
    }
    if sources.is_empty() {
        bail!("no matching source configurations found");
    }

    let mut state = load_state(&options.state_path)?;
    let mut reports = Vec::new();

    for source in sources {
        if !source.config.source.enabled {
            info!(source = %source.config.source.key, "source disabled; skipping");
            continue;
        }

        info!(source = %source.config.source.key, "sync start");
        let docs = fetch_source_documents(&source)
            .with_context(|| format!("fetch failed for source {}", source.config.source.key))?;
        let candidates = parse_source_events(&source, &docs)
            .with_context(|| format!("parse failed for source {}", source.config.source.key))?;

        let mut report = SourceRunReport {
            source_key: source.config.source.key.clone(),
            pages_fetched: docs.len(),
            records_parsed: candidates.len(),
            ..SourceRunReport::default()
        };

        let changed_years = merge_source_events(&mut state, &source, candidates, &mut report)?;

        info!(
            source = %source.config.source.key,
            inserted = report.inserted,
            updated = report.updated,
            unchanged = report.unchanged,
            cancelled = report.cancelled,
            changed_years = ?changed_years,
            "sync merge complete"
        );

        if !options.dry_run {
            rebuild_source_calendars(&state, &source, &options.out_dir, None, Some(changed_years))?;
        }

        reports.push(report);
    }

    if !options.dry_run {
        save_state(&options.state_path, &state)?;
        info!(state = %options.state_path.display(), "state written");
    } else {
        info!("dry run enabled; state and calendars not persisted");
    }

    Ok(reports)
}

pub fn build_calendars(options: &BuildOptions) -> Result<()> {
    let mut sources = load_sources_from_dir(&options.config_dir)?;
    if let Some(filter) = &options.source {
        sources.retain(|s| s.config.source.key == *filter);
    }
    if sources.is_empty() {
        bail!("no matching source configurations found");
    }

    let state = load_state(&options.state_path)?;
    for source in sources {
        rebuild_source_calendars(&state, &source, &options.out_dir, options.year, None)?;
    }

    Ok(())
}

pub fn validate_configs(options: &ValidateOptions) -> Result<Vec<String>> {
    let mut messages = Vec::new();

    if let Some(file) = &options.source_file {
        let source = load_source_file(file)?;
        messages.push(format!(
            "OK: {} ({})",
            source.config.source.key,
            file.display()
        ));
        return Ok(messages);
    }

    if let Some(dir) = &options.config_dir {
        let sources = load_sources_from_dir(dir)?;
        for source in sources {
            messages.push(format!(
                "OK: {} ({})",
                source.config.source.key,
                source.path.display()
            ));
        }
        return Ok(messages);
    }

    bail!("either --config-dir or --source-file must be provided");
}

pub fn load_state_for_read(path: &Path) -> Result<State> {
    load_state(path)
}

fn merge_source_events(
    state: &mut State,
    source: &LoadedSource,
    candidates: Vec<CandidateEvent>,
    report: &mut SourceRunReport,
) -> Result<BTreeSet<i32>> {
    let now = Utc::now();
    let today = now.date_naive();
    let source_key = source.config.source.key.as_str();

    let mut seen_uids = HashSet::new();
    let mut changed_years = BTreeSet::new();

    for mut candidate in candidates {
        candidate.categories.sort();
        candidate.categories.dedup();

        let uid = stable_uid(&candidate);
        let revision_hash = revision_hash(&candidate)?;
        let year_bucket = candidate.time.year_bucket();
        seen_uids.insert(uid.clone());

        if let Some(existing) = state.events.get_mut(&uid) {
            if existing.revision_hash != revision_hash {
                let created_at = existing.created_at;
                let new_sequence = existing.sequence.saturating_add(1);
                *existing = candidate_to_record(
                    candidate,
                    uid,
                    revision_hash,
                    new_sequence,
                    created_at,
                    now,
                );
                report.updated += 1;
                if let Some(year) = year_bucket {
                    changed_years.insert(year);
                }
            } else {
                existing.last_seen_at = now;
                report.unchanged += 1;
            }
        } else {
            let record = candidate_to_record(candidate, uid.clone(), revision_hash, 0, now, now);
            if let Some(year) = record.year_bucket() {
                changed_years.insert(year);
            }
            state.events.insert(uid, record);
            report.inserted += 1;
        }
    }

    for event in state
        .events
        .values_mut()
        .filter(|event| event.source_key == source_key)
    {
        if seen_uids.contains(&event.uid) {
            continue;
        }
        if !event.is_future_relative_to(today) {
            continue;
        }
        if event.status.eq_ignore_ascii_case("cancelled") {
            continue;
        }

        event.status = "cancelled".to_string();
        event.sequence = event.sequence.saturating_add(1);
        event.last_modified = now;
        event.last_seen_at = now;
        report.cancelled += 1;

        if let Some(year) = event.year_bucket() {
            changed_years.insert(year);
        }
    }

    Ok(changed_years)
}

fn candidate_to_record(
    candidate: CandidateEvent,
    uid: String,
    revision_hash: String,
    sequence: u32,
    created_at: chrono::DateTime<Utc>,
    now: chrono::DateTime<Utc>,
) -> EventRecord {
    EventRecord {
        uid,
        source_key: candidate.source_key,
        source_name: candidate.source_name,
        source_event_id: candidate.source_event_id,
        source_url: candidate.source_url,
        title: candidate.title,
        description: candidate.description,
        time: candidate.time,
        timezone: candidate.timezone,
        status: candidate.status,
        event_type: candidate.event_type,
        subtype: candidate.subtype,
        categories: candidate.categories,
        jurisdiction: candidate.jurisdiction,
        country: candidate.country,
        importance: candidate.importance,
        confidence: candidate.confidence,
        metadata: candidate.metadata,
        sequence,
        revision_hash,
        created_at,
        last_modified: now,
        last_seen_at: now,
    }
}

#[derive(Serialize)]
struct RevisionMaterial<'a> {
    source_key: &'a str,
    source_event_id: &'a Option<String>,
    source_url: &'a Option<String>,
    title: &'a str,
    description: &'a Option<String>,
    time: &'a crate::model::EventTimeSpec,
    status: &'a str,
    event_type: &'a str,
    subtype: &'a Option<String>,
    categories: &'a [String],
    metadata: &'a BTreeMap<String, String>,
}

fn revision_hash(candidate: &CandidateEvent) -> Result<String> {
    let material = RevisionMaterial {
        source_key: &candidate.source_key,
        source_event_id: &candidate.source_event_id,
        source_url: &candidate.source_url,
        title: &candidate.title,
        description: &candidate.description,
        time: &candidate.time,
        status: &candidate.status,
        event_type: &candidate.event_type,
        subtype: &candidate.subtype,
        categories: &candidate.categories,
        metadata: &candidate.metadata,
    };

    let json = serde_json::to_vec(&material)?;
    let digest = Sha256::digest(json);
    Ok(hex::encode(digest))
}

fn stable_uid(candidate: &CandidateEvent) -> String {
    let identity = if let Some(source_event_id) = &candidate.source_event_id {
        format!("{}::{}", candidate.source_key, source_event_id)
    } else if let Some(url) = &candidate.source_url {
        format!("{}::{}", candidate.source_key, url)
    } else {
        format!(
            "{}::{}::{}",
            candidate.source_key,
            candidate.title.to_lowercase(),
            candidate
                .time
                .year_bucket()
                .map(|y| y.to_string())
                .unwrap_or_else(|| "undated".to_string())
        )
    };

    let digest = Sha256::digest(identity.as_bytes());
    let short = &hex::encode(digest)[..24];
    format!("{short}@rics.local")
}

fn rebuild_source_calendars(
    state: &State,
    source: &LoadedSource,
    out_dir: &Path,
    year_filter: Option<i32>,
    changed_years: Option<BTreeSet<i32>>,
) -> Result<()> {
    let mut by_year: HashMap<i32, Vec<&EventRecord>> = HashMap::new();
    for event in state.events.values().filter(|event| {
        event.source_key == source.config.source.key
            && !event.status.eq_ignore_ascii_case("cancelled")
    }) {
        if let Some(year) = event.year_bucket() {
            by_year.entry(year).or_default().push(event);
        }
    }

    if let Some(year) = year_filter {
        by_year.retain(|y, _| *y == year);
    }

    if let Some(changed) = &changed_years {
        by_year.retain(|year, _| changed.contains(year));
    }

    let source_dir = out_dir
        .join("sources")
        .join(source.config.sanitized_source_dir_name());
    let file_prefix = source.config.sanitized_source_dir_name();
    std::fs::create_dir_all(&source_dir)
        .with_context(|| format!("failed to create output dir {}", source_dir.display()))?;

    for (year, mut events) in by_year {
        events.sort_by(|a, b| {
            let a_key = event_sort_key(a);
            let b_key = event_sort_key(b);
            a_key.cmp(&b_key)
        });
        let path = source_dir.join(ics_filename(&file_prefix, year));
        write_source_year_calendar(&source.config, year, &events, &path)?;
        info!(
            source = %source.config.source.key,
            year,
            events = events.len(),
            file = %path.display(),
            "calendar file rebuilt"
        );
    }

    if source_dir.exists() {
        cleanup_stale_year_files(
            &source_dir,
            state,
            &source.config.source.key,
            &file_prefix,
            year_filter,
        )?;
    }

    Ok(())
}

fn cleanup_stale_year_files(
    source_dir: &Path,
    state: &State,
    source_key: &str,
    file_prefix: &str,
    year_filter: Option<i32>,
) -> Result<()> {
    let mut existing_years = HashSet::new();
    for event in state
        .events
        .values()
        .filter(|event| event.source_key == source_key)
    {
        if let Some(year) = event.year_bucket() {
            existing_years.insert(year);
        }
    }

    if let Some(year) = year_filter {
        existing_years.retain(|v| *v == year);
    }

    for entry in std::fs::read_dir(source_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|v| v.to_str()) != Some("ics") {
            continue;
        }
        let Some(file_name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if is_legacy_year_only_filename(file_name) {
            std::fs::remove_file(&path)
                .with_context(|| format!("failed to remove legacy file {}", path.display()))?;
            warn!(file = %path.display(), "removed legacy calendar file");
            continue;
        }

        let Some(file_year) = parse_year_from_filename(file_name, file_prefix) else {
            continue;
        };

        if !existing_years.contains(&file_year) {
            std::fs::remove_file(&path)
                .with_context(|| format!("failed to remove stale file {}", path.display()))?;
            warn!(file = %path.display(), "removed stale calendar file");
        }
    }

    Ok(())
}

fn ics_filename(file_prefix: &str, year: i32) -> String {
    format!("{file_prefix}-{year}.ics")
}

fn parse_year_from_filename(file_name: &str, file_prefix: &str) -> Option<i32> {
    let prefixed = format!("{file_prefix}-");
    let stem = file_name.strip_suffix(".ics")?;
    let year = stem.strip_prefix(&prefixed)?;
    year.parse::<i32>().ok()
}

fn is_legacy_year_only_filename(file_name: &str) -> bool {
    file_name
        .strip_suffix(".ics")
        .and_then(|stem| stem.parse::<i32>().ok())
        .is_some()
}

fn event_sort_key(event: &EventRecord) -> String {
    let day = event
        .time
        .start_date()
        .map(|d| d.to_string())
        .unwrap_or_else(|| "9999-12-31".to_string());
    format!("{day}|{}", event.uid)
}
