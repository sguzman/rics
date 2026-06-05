use crate::config::{LoadedBundle, LoadedSource, sanitize_for_path};
use crate::model::{EventRecord, EventTimeSpec, State};
use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use jsonschema::{Draft, JSONSchema};
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

const SNAPSHOT_SCHEMA_VERSION: u32 = 1;
const SNAPSHOT_SCHEMA_FILE: &str = "schemas/rics-snapshot.schema.json";
const SNAPSHOT_OUTPUT_DIR: &str = "snapshots";
const SNAPSHOT_OUTPUT_FILE: &str = "rics-all.json";
const SNAPSHOT_OUTPUT_SCHEMA_FILE: &str = "rics-all.schema.json";
const FACET_METADATA_KEYS: &[&str] = &[
    "game",
    "circuit",
    "league",
    "office",
    "round",
    "election_kind",
    "surface",
    "region",
    "source_class",
];

#[derive(Debug, Serialize)]
pub struct SnapshotDocument {
    pub schema_version: u32,
    pub generated_at: DateTime<Utc>,
    pub state_schema_version: u32,
    pub stats: SnapshotStats,
    pub sources: BTreeMap<String, SnapshotSource>,
    pub bundles: BTreeMap<String, SnapshotBundle>,
    pub calendars: BTreeMap<String, SnapshotCalendar>,
    pub events: BTreeMap<String, SnapshotEvent>,
    pub indexes: SnapshotIndexes,
}

#[derive(Debug, Serialize)]
pub struct SnapshotStats {
    pub source_count: usize,
    pub bundle_count: usize,
    pub calendar_count: usize,
    pub event_count: usize,
    pub cancelled_event_count: usize,
    pub years: Vec<i32>,
    pub tag_count: usize,
    pub ontology_path_count: usize,
    pub domain_count: usize,
    pub country_count: usize,
}

#[derive(Debug, Serialize)]
pub struct SnapshotSource {
    pub key: String,
    pub name: String,
    pub domain: String,
    pub timezone: Option<String>,
    pub jurisdiction: Option<String>,
    pub default_country: Option<String>,
    pub config_path: String,
    pub categories: Vec<String>,
    pub event_type: String,
    pub subtype: Option<String>,
    pub split_by_country: bool,
    pub tags: Vec<String>,
    pub ontology_paths: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct SnapshotBundle {
    pub key: String,
    pub name: String,
    pub source_patterns: Vec<String>,
    pub config_path: String,
    pub tags: Vec<String>,
    pub ontology_paths: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct SnapshotCalendar {
    pub id: String,
    pub kind: String,
    pub origin_key: String,
    pub name: String,
    pub description: Option<String>,
    pub tags: Vec<String>,
    pub ontology_paths: Vec<String>,
    pub facets: BTreeMap<String, Vec<String>>,
    pub years: Vec<i32>,
    pub event_ids_by_year: BTreeMap<String, Vec<String>>,
    pub undated_event_ids: Vec<String>,
    pub event_count: usize,
    pub ics_files: BTreeMap<String, Vec<String>>,
}

#[derive(Debug, Serialize)]
pub struct SnapshotEvent {
    pub uid: String,
    pub source_key: String,
    pub source_name: String,
    pub source_event_id: Option<String>,
    pub source_url: Option<String>,
    pub title: String,
    pub description: Option<String>,
    pub time: EventTimeSpec,
    pub timezone: Option<String>,
    pub status: String,
    pub event_type: String,
    pub subtype: Option<String>,
    pub categories: Vec<String>,
    pub jurisdiction: Option<String>,
    pub country: Option<String>,
    pub importance: Option<u8>,
    pub confidence: Option<f32>,
    pub metadata: BTreeMap<String, String>,
    pub sequence: u32,
    pub revision_hash: String,
    pub created_at: DateTime<Utc>,
    pub last_modified: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
    pub year_bucket: Option<i32>,
    pub time_precision: String,
    pub is_cancelled: bool,
    pub calendar_ids: Vec<String>,
    pub tags: Vec<String>,
    pub ontology_paths: Vec<String>,
    pub facets: BTreeMap<String, Vec<String>>,
}

#[derive(Debug, Serialize)]
pub struct SnapshotIndexes {
    pub years: BTreeMap<String, SnapshotIndexMembership>,
    pub tags: BTreeMap<String, SnapshotIndexMembership>,
    pub ontology_paths: BTreeMap<String, SnapshotIndexMembership>,
    pub countries: BTreeMap<String, SnapshotIndexMembership>,
    pub domains: BTreeMap<String, SnapshotIndexMembership>,
    pub undated: SnapshotIndexMembership,
}

#[derive(Debug, Serialize, Default)]
pub struct SnapshotIndexMembership {
    pub event_ids: Vec<String>,
    pub calendar_ids: Vec<String>,
}

pub fn export_snapshot(
    state: &State,
    sources: &[LoadedSource],
    bundles: &[LoadedBundle],
    out_dir: &Path,
) -> Result<()> {
    let generated_at = Utc::now();
    let snapshot = build_snapshot_document(state, sources, bundles, out_dir, generated_at)?;
    let schema_path = schema_source_path();
    let schema = load_schema(&schema_path)?;
    let snapshot_value = serde_json::to_value(&snapshot)?;
    validate_snapshot_value(&schema, &snapshot_value)?;

    let snapshots_dir = out_dir.join(SNAPSHOT_OUTPUT_DIR);
    fs::create_dir_all(&snapshots_dir)
        .with_context(|| format!("failed to create snapshot dir {}", snapshots_dir.display()))?;

    let snapshot_path = snapshots_dir.join(SNAPSHOT_OUTPUT_FILE);
    fs::write(&snapshot_path, serde_json::to_string_pretty(&snapshot)? + "\n")
        .with_context(|| format!("failed to write snapshot {}", snapshot_path.display()))?;

    let schema_output = snapshots_dir.join(SNAPSHOT_OUTPUT_SCHEMA_FILE);
    fs::copy(&schema_path, &schema_output).with_context(|| {
        format!(
            "failed to copy snapshot schema {} to {}",
            schema_path.display(),
            schema_output.display()
        )
    })?;

    Ok(())
}

fn build_snapshot_document(
    state: &State,
    sources: &[LoadedSource],
    bundles: &[LoadedBundle],
    out_dir: &Path,
    generated_at: DateTime<Utc>,
) -> Result<SnapshotDocument> {
    let source_lookup = sources
        .iter()
        .map(|source| (source.config.source.key.clone(), source))
        .collect::<HashMap<_, _>>();
    let mut source_domain_by_key = HashMap::<String, String>::new();
    let mut source_name_by_key = HashMap::<String, String>::new();
    let mut split_by_country_by_key = HashMap::<String, bool>::new();
    let mut default_country_by_key = HashMap::<String, Option<String>>::new();

    let mut sources_out = BTreeMap::new();
    for source in sources {
        source_domain_by_key.insert(
            source.config.source.key.clone(),
            source.config.source.domain.clone(),
        );
        source_name_by_key.insert(
            source.config.source.key.clone(),
            source.config.source.name.clone(),
        );
        split_by_country_by_key.insert(
            source.config.source.key.clone(),
            source.config.publish.split_by_country,
        );
        default_country_by_key.insert(
            source.config.source.key.clone(),
            source.config.source.default_country.clone(),
        );
        let mut tags = BTreeSet::new();
        let mut ontology = BTreeSet::new();
        derive_source_tags_and_paths(source, &mut tags, &mut ontology);
        sources_out.insert(
            source.config.source.key.clone(),
            SnapshotSource {
                key: source.config.source.key.clone(),
                name: source.config.source.name.clone(),
                domain: source.config.source.domain.clone(),
                timezone: source.config.source.timezone.clone(),
                jurisdiction: source.config.source.jurisdiction.clone(),
                default_country: source.config.source.default_country.clone(),
                config_path: display_repo_relative(&source.path),
                categories: sorted_unique(source.config.event.categories.clone()),
                event_type: source.config.event.event_type.clone(),
                subtype: source.config.event.subtype.clone(),
                split_by_country: source.config.publish.split_by_country,
                tags: tags.into_iter().collect(),
                ontology_paths: ontology.into_iter().collect(),
            },
        );
    }

    for event in state.events.values() {
        if sources_out.contains_key(&event.source_key) {
            continue;
        }
        let domain = event
            .source_key
            .split('.')
            .next()
            .unwrap_or("unknown")
            .to_string();
        source_domain_by_key.insert(event.source_key.clone(), domain.clone());
        source_name_by_key.insert(event.source_key.clone(), event.source_name.clone());
        split_by_country_by_key.insert(event.source_key.clone(), false);
        default_country_by_key.insert(event.source_key.clone(), event.country.clone());

        let mut tags = BTreeSet::new();
        let mut ontology = BTreeSet::new();
        add_tag(&mut tags, &domain);
        add_tag(&mut tags, &event.event_type);
        for part in event.source_key.split('.') {
            add_tag(&mut tags, part);
        }
        add_path(&mut ontology, "source", event.source_key.split('.'));
        add_path(&mut ontology, "domain", [domain.as_str()]);
        add_path(&mut ontology, "semantic", [event.event_type.as_str()]);
        if let Some(subtype) = event.subtype.as_deref() {
            add_path(&mut ontology, "semantic", [event.event_type.as_str(), subtype]);
        }
        if let Some(country) = event.country.as_deref() {
            add_path(&mut ontology, "geo", [country]);
        }

        sources_out.insert(
            event.source_key.clone(),
            SnapshotSource {
                key: event.source_key.clone(),
                name: event.source_name.clone(),
                domain,
                timezone: event.timezone.clone(),
                jurisdiction: event.jurisdiction.clone(),
                default_country: event.country.clone(),
                config_path: String::new(),
                categories: sorted_unique(event.categories.clone()),
                event_type: event.event_type.clone(),
                subtype: event.subtype.clone(),
                split_by_country: false,
                tags: tags.into_iter().collect(),
                ontology_paths: ontology.into_iter().collect(),
            },
        );
    }

    let mut bundles_out = BTreeMap::new();
    for bundle in bundles {
        let mut tags = BTreeSet::new();
        let mut ontology = BTreeSet::new();
        derive_bundle_tags_and_paths(bundle, &mut tags, &mut ontology);
        bundles_out.insert(
            bundle.config.bundle.key.clone(),
            SnapshotBundle {
                key: bundle.config.bundle.key.clone(),
                name: bundle.config.bundle.name.clone(),
                source_patterns: bundle.config.include.source_patterns.clone(),
                config_path: display_repo_relative(&bundle.path),
                tags: tags.into_iter().collect(),
                ontology_paths: ontology.into_iter().collect(),
            },
        );
    }

    let mut event_to_calendars: HashMap<String, BTreeSet<String>> = HashMap::new();
    let mut calendar_to_events: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut calendar_to_origin: BTreeMap<String, (String, String)> = BTreeMap::new();

    for event in state.events.values() {
        let split_by_country = *split_by_country_by_key
            .get(&event.source_key)
            .unwrap_or(&false);
        let calendar_id = if split_by_country {
            let country = event
                .country
                .clone()
                .or_else(|| default_country_by_key.get(&event.source_key).cloned().flatten())
                .unwrap_or_else(|| "xx".to_string())
                .to_ascii_lowercase();
            format!("source:{}:country:{country}", event.source_key)
        } else {
            format!("source:{}", event.source_key)
        };
        event_to_calendars
            .entry(event.uid.clone())
            .or_default()
            .insert(calendar_id.clone());
        calendar_to_events
            .entry(calendar_id.clone())
            .or_default()
            .insert(event.uid.clone());
        calendar_to_origin
            .entry(calendar_id)
            .or_insert_with(|| ("source".to_string(), event.source_key.clone()));
    }

    for bundle in bundles {
        let calendar_id = format!("bundle:{}", bundle.config.bundle.key);
        calendar_to_origin
            .entry(calendar_id.clone())
            .or_insert_with(|| ("bundle".to_string(), bundle.config.bundle.key.clone()));
        for event in state.events.values() {
            if source_key_matches_any_pattern(&event.source_key, &bundle.config.include.source_patterns)
            {
                event_to_calendars
                    .entry(event.uid.clone())
                    .or_default()
                    .insert(calendar_id.clone());
                calendar_to_events
                    .entry(calendar_id.clone())
                    .or_default()
                    .insert(event.uid.clone());
            }
        }
    }

    let mut events_out = BTreeMap::new();
    let mut tag_index = BTreeMap::<String, SnapshotIndexMembership>::new();
    let mut ontology_index = BTreeMap::<String, SnapshotIndexMembership>::new();
    let mut year_index = BTreeMap::<String, SnapshotIndexMembership>::new();
    let mut country_index = BTreeMap::<String, SnapshotIndexMembership>::new();
    let mut domain_index = BTreeMap::<String, SnapshotIndexMembership>::new();
    let mut undated_index = SnapshotIndexMembership::default();

    for event in state.events.values() {
        let domain = source_domain_by_key
            .get(&event.source_key)
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        let calendar_ids = event_to_calendars
            .get(&event.uid)
            .map(|ids| ids.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        let facets = derive_event_facets(event, &domain, &calendar_ids);
        let tags = derive_tags_for_event(event, &domain);
        let ontology_paths = derive_ontology_for_event(event, &domain);
        let year_bucket = event.year_bucket();

        for tag in &tags {
            insert_index_membership(&mut tag_index, tag, &event.uid, &calendar_ids);
        }
        for path in &ontology_paths {
            insert_index_membership(&mut ontology_index, path, &event.uid, &calendar_ids);
        }
        if let Some(year) = year_bucket {
            insert_index_membership(&mut year_index, &year.to_string(), &event.uid, &calendar_ids);
        } else {
            undated_index.event_ids.push(event.uid.clone());
            extend_unique(&mut undated_index.calendar_ids, calendar_ids.clone());
        }
        insert_optional_scalar_index(
            &mut country_index,
            event.country.as_deref(),
            &event.uid,
            &calendar_ids,
        );
        insert_index_membership(
            &mut domain_index,
            &domain,
            &event.uid,
            &calendar_ids,
        );

        events_out.insert(
            event.uid.clone(),
            SnapshotEvent {
                uid: event.uid.clone(),
                source_key: event.source_key.clone(),
                source_name: event.source_name.clone(),
                source_event_id: event.source_event_id.clone(),
                source_url: event.source_url.clone(),
                title: event.title.clone(),
                description: event.description.clone(),
                time: event.time.clone(),
                timezone: event.timezone.clone(),
                status: event.status.clone(),
                event_type: event.event_type.clone(),
                subtype: event.subtype.clone(),
                categories: sorted_unique(event.categories.clone()),
                jurisdiction: event.jurisdiction.clone(),
                country: event.country.clone(),
                importance: event.importance,
                confidence: event.confidence,
                metadata: event.metadata.clone(),
                sequence: event.sequence,
                revision_hash: event.revision_hash.clone(),
                created_at: event.created_at,
                last_modified: event.last_modified,
                last_seen_at: event.last_seen_at,
                year_bucket,
                time_precision: event.time.precision().to_string(),
                is_cancelled: event.status.eq_ignore_ascii_case("cancelled"),
                calendar_ids,
                tags,
                ontology_paths,
                facets,
            },
        );
    }

    let mut calendars_out = BTreeMap::new();
    for (calendar_id, event_ids_set) in &calendar_to_events {
        let (kind, origin_key) = calendar_to_origin
            .get(calendar_id)
            .cloned()
            .ok_or_else(|| anyhow!("missing origin for calendar {}", calendar_id))?;
        let event_ids = event_ids_set.iter().cloned().collect::<Vec<_>>();
        let events = event_ids
            .iter()
            .filter_map(|id| state.events.get(id))
            .collect::<Vec<_>>();
        let years = sorted_unique(
            events
                .iter()
                .filter_map(|event| event.year_bucket())
                .collect::<Vec<_>>(),
        );

        let mut event_ids_by_year = BTreeMap::<String, Vec<String>>::new();
        let mut undated_event_ids = Vec::new();
        let mut tags = BTreeSet::new();
        let mut ontology = BTreeSet::new();
        let mut facets = BTreeMap::<String, BTreeSet<String>>::new();

        for event in &events {
            if let Some(snapshot_event) = events_out.get(&event.uid) {
                if let Some(year) = snapshot_event.year_bucket {
                    event_ids_by_year
                        .entry(year.to_string())
                        .or_default()
                        .push(event.uid.clone());
                } else {
                    undated_event_ids.push(event.uid.clone());
                }
                for tag in &snapshot_event.tags {
                    tags.insert(tag.clone());
                }
                for path in &snapshot_event.ontology_paths {
                    ontology.insert(path.clone());
                }
                merge_facets(&mut facets, &snapshot_event.facets);
            }
        }
        for ids in event_ids_by_year.values_mut() {
            ids.sort();
            ids.dedup();
        }
        undated_event_ids.sort();
        undated_event_ids.dedup();

        let ics_files = derive_calendar_ics_files(
            out_dir,
            calendar_id,
            &kind,
            &origin_key,
            &years,
            source_lookup.get(&origin_key).copied(),
            bundles.iter().find(|bundle| bundle.config.bundle.key == origin_key),
        );

        let (name, description) = if kind == "source" {
            let source = source_lookup
                .get(&origin_key);
            if let Some(country) = calendar_id.split(":country:").nth(1) {
                (
                    format!(
                        "{} ({})",
                        source
                            .map(|value| value.config.source.name.clone())
                            .or_else(|| source_name_by_key.get(&origin_key).cloned())
                            .unwrap_or_else(|| origin_key.clone()),
                        country.to_ascii_uppercase()
                    ),
                    Some(format!(
                        "Logical calendar for source {} filtered to country {}.",
                        origin_key,
                        country.to_ascii_uppercase()
                    )),
                )
            } else {
                (
                    source
                        .map(|value| value.config.source.name.clone())
                        .or_else(|| source_name_by_key.get(&origin_key).cloned())
                        .unwrap_or_else(|| origin_key.clone()),
                    Some(format!("Logical calendar for source {}.", origin_key)),
                )
            }
        } else {
            let bundle = bundles
                .iter()
                .find(|bundle| bundle.config.bundle.key == origin_key)
                .ok_or_else(|| anyhow!("missing bundle for calendar {}", calendar_id))?;
            (
                bundle.config.bundle.name.clone(),
                Some(format!(
                    "Aggregate calendar for bundle {}.",
                    bundle.config.bundle.key
                )),
            )
        };

        let tags_vec = tags.into_iter().collect::<Vec<_>>();
        let ontology_vec = ontology.into_iter().collect::<Vec<_>>();
        let facet_map = finalize_facet_map(facets);

        for year in &years {
            if let Some(index) = year_index.get_mut(&year.to_string()) {
                insert_unique_sorted(&mut index.calendar_ids, calendar_id.clone());
            }
        }
        if !undated_event_ids.is_empty() {
            insert_unique_sorted(&mut undated_index.calendar_ids, calendar_id.clone());
        }
        for tag in &tags_vec {
            if let Some(index) = tag_index.get_mut(tag) {
                insert_unique_sorted(&mut index.calendar_ids, calendar_id.clone());
            }
        }
        for path in &ontology_vec {
            if let Some(index) = ontology_index.get_mut(path) {
                insert_unique_sorted(&mut index.calendar_ids, calendar_id.clone());
            }
        }
        if let Some(values) = facet_map.get("country") {
            for country in values {
                if let Some(index) = country_index.get_mut(country) {
                    insert_unique_sorted(&mut index.calendar_ids, calendar_id.clone());
                }
            }
        }
        if let Some(values) = facet_map.get("domain") {
            for domain in values {
                if let Some(index) = domain_index.get_mut(domain) {
                    insert_unique_sorted(&mut index.calendar_ids, calendar_id.clone());
                }
            }
        }

        calendars_out.insert(
            calendar_id.clone(),
            SnapshotCalendar {
                id: calendar_id.clone(),
                kind,
                origin_key,
                name,
                description,
                tags: tags_vec,
                ontology_paths: ontology_vec,
                facets: facet_map,
                years,
                event_ids_by_year,
                undated_event_ids,
                event_count: event_ids.len(),
                ics_files,
            },
        );
    }

    normalize_index_memberships(&mut tag_index);
    normalize_index_memberships(&mut ontology_index);
    normalize_index_memberships(&mut year_index);
    normalize_index_memberships(&mut country_index);
    normalize_index_memberships(&mut domain_index);
    normalize_membership(&mut undated_index);

    let stats = SnapshotStats {
        source_count: sources_out.len(),
        bundle_count: bundles_out.len(),
        calendar_count: calendars_out.len(),
        event_count: events_out.len(),
        cancelled_event_count: events_out.values().filter(|event| event.is_cancelled).count(),
        years: sorted_unique(
            events_out
                .values()
                .filter_map(|event| event.year_bucket)
                .collect::<Vec<_>>(),
        ),
        tag_count: tag_index.len(),
        ontology_path_count: ontology_index.len(),
        domain_count: domain_index.len(),
        country_count: country_index.len(),
    };

    Ok(SnapshotDocument {
        schema_version: SNAPSHOT_SCHEMA_VERSION,
        generated_at,
        state_schema_version: state.schema_version,
        stats,
        sources: sources_out,
        bundles: bundles_out,
        calendars: calendars_out,
        events: events_out,
        indexes: SnapshotIndexes {
            years: year_index,
            tags: tag_index,
            ontology_paths: ontology_index,
            countries: country_index,
            domains: domain_index,
            undated: undated_index,
        },
    })
}

fn derive_source_tags_and_paths(
    source: &LoadedSource,
    tags: &mut BTreeSet<String>,
    ontology: &mut BTreeSet<String>,
) {
    add_tag(tags, &source.config.source.domain);
    add_tag(tags, &source.config.event.event_type);
    if let Some(subtype) = &source.config.event.subtype {
        add_tag(tags, subtype);
    }
    if let Some(country) = &source.config.source.default_country {
        add_tag(tags, country);
    }
    if let Some(jurisdiction) = &source.config.source.jurisdiction {
        add_tag(tags, jurisdiction);
    }
    for part in source.config.source.key.split('.') {
        add_tag(tags, part);
    }
    for category in &source.config.event.categories {
        add_tag(tags, category);
    }

    add_path(ontology, "source", source.config.source.key.split('.'));
    add_path(ontology, "domain", [source.config.source.domain.as_str()]);
    add_path(ontology, "semantic", [source.config.event.event_type.as_str()]);
    if let Some(subtype) = source.config.event.subtype.as_deref() {
        add_path(
            ontology,
            "semantic",
            [source.config.event.event_type.as_str(), subtype],
        );
    }
    if let Some(country) = source.config.source.default_country.as_deref() {
        add_path(ontology, "geo", [country]);
    }
}

fn derive_bundle_tags_and_paths(
    bundle: &LoadedBundle,
    tags: &mut BTreeSet<String>,
    ontology: &mut BTreeSet<String>,
) {
    for part in bundle.config.bundle.key.split('.') {
        add_tag(tags, part);
    }
    add_path(ontology, "bundle", bundle.config.bundle.key.split('.'));
}

fn derive_tags_for_event(event: &EventRecord, domain: &str) -> Vec<String> {
    let mut tags = BTreeSet::new();
    add_tag(&mut tags, domain);
    add_tag(&mut tags, &event.event_type);
    if let Some(subtype) = &event.subtype {
        add_tag(&mut tags, subtype);
    }
    if let Some(country) = &event.country {
        add_tag(&mut tags, country);
    }
    if let Some(jurisdiction) = &event.jurisdiction {
        add_tag(&mut tags, jurisdiction);
    }
    for category in &event.categories {
        add_tag(&mut tags, category);
    }
    for part in event.source_key.split('.') {
        add_tag(&mut tags, part);
    }
    for key in FACET_METADATA_KEYS {
        if let Some(value) = event.metadata.get(*key) {
            add_tag(&mut tags, value);
        }
    }
    tags.into_iter().collect()
}

fn derive_ontology_for_event(event: &EventRecord, domain: &str) -> Vec<String> {
    let mut ontology = BTreeSet::new();
    add_path(&mut ontology, "source", event.source_key.split('.'));
    add_path(&mut ontology, "domain", [domain]);
    add_path(&mut ontology, "semantic", [event.event_type.as_str()]);
    if let Some(subtype) = event.subtype.as_deref() {
        add_path(&mut ontology, "semantic", [event.event_type.as_str(), subtype]);
    }
    if let Some(country) = event.country.as_deref() {
        add_path(&mut ontology, "geo", [country]);
    } else if let Some(jurisdiction) = event.jurisdiction.as_deref() {
        add_path(&mut ontology, "geo", [jurisdiction]);
    }
    for key in FACET_METADATA_KEYS {
        if let Some(value) = event.metadata.get(*key) {
            add_path(&mut ontology, key, [value.as_str()]);
        }
    }
    ontology.into_iter().collect()
}

fn derive_event_facets(
    event: &EventRecord,
    domain: &str,
    calendar_ids: &[String],
) -> BTreeMap<String, Vec<String>> {
    let mut facets = BTreeMap::<String, BTreeSet<String>>::new();
    insert_facet(&mut facets, "domain", domain);
    insert_facet(&mut facets, "event_type", &event.event_type);
    if let Some(subtype) = &event.subtype {
        insert_facet(&mut facets, "subtype", subtype);
    }
    if let Some(country) = &event.country {
        insert_facet(&mut facets, "country", country);
    }
    if let Some(jurisdiction) = &event.jurisdiction {
        insert_facet(&mut facets, "jurisdiction", jurisdiction);
    }
    insert_facet(&mut facets, "source_key", &event.source_key);
    for calendar_id in calendar_ids {
        if let Some(bundle_key) = calendar_id.strip_prefix("bundle:") {
            insert_facet(&mut facets, "bundle_keys", bundle_key);
        }
    }
    for key in FACET_METADATA_KEYS {
        if let Some(value) = event.metadata.get(*key) {
            insert_facet(&mut facets, key, value);
        }
    }
    finalize_facet_map(facets)
}

fn derive_calendar_ics_files(
    out_dir: &Path,
    calendar_id: &str,
    kind: &str,
    origin_key: &str,
    years: &[i32],
    source: Option<&LoadedSource>,
    bundle: Option<&LoadedBundle>,
) -> BTreeMap<String, Vec<String>> {
    let mut files = BTreeMap::new();

    if kind == "source" {
        let Some(source) = source else {
            return files;
        };
        let source_dir_name = source.config.sanitized_source_dir_name();
        let country = calendar_id.split(":country:").nth(1);
        for year in years {
            let file_name = source_ics_filename(source, &source_dir_name, *year, country);
            let relative = PathBuf::from("sources")
                .join(&source_dir_name)
                .join(&file_name);
            if out_dir.join(&relative).exists() {
                files.insert(year.to_string(), vec![path_to_string(&relative)]);
            }
        }
    } else {
        let Some(bundle) = bundle else {
            return files;
        };
        let bundle_dir_name = bundle.config.sanitized_bundle_dir_name();
        for year in years {
            let file_name = bundle_ics_filename(bundle, &bundle_dir_name, *year);
            let relative = PathBuf::from("bundles")
                .join(&bundle_dir_name)
                .join(&file_name);
            if out_dir.join(&relative).exists() {
                files.insert(year.to_string(), vec![path_to_string(&relative)]);
            }
        }
    }

    let _ = origin_key;
    files
}

fn schema_source_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(SNAPSHOT_SCHEMA_FILE)
}

fn load_schema(path: &Path) -> Result<Value> {
    let schema = fs::read_to_string(path)
        .with_context(|| format!("failed to read snapshot schema {}", path.display()))?;
    serde_json::from_str(&schema)
        .with_context(|| format!("failed to parse snapshot schema {}", path.display()))
}

fn validate_snapshot_value(schema: &Value, instance: &Value) -> Result<()> {
    let leaked_schema = Box::leak(Box::new(schema.clone()));
    let compiled = JSONSchema::options()
        .with_draft(Draft::Draft202012)
        .compile(leaked_schema)
        .context("failed to compile snapshot json schema")?;
    if let Err(errors) = compiled.validate(instance) {
        let details = errors.map(|error| error.to_string()).collect::<Vec<_>>();
        bail!("snapshot json validation failed: {}", details.join("; "));
    }
    Ok(())
}

fn display_repo_relative(path: &Path) -> String {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    path.strip_prefix(manifest_dir)
        .map(path_to_string)
        .unwrap_or_else(|_| path.display().to_string())
}

fn path_to_string(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn source_key_matches_any_pattern(source_key: &str, patterns: &[String]) -> bool {
    patterns.iter().any(|pattern| {
        if let Some(prefix) = pattern.strip_suffix('*') {
            source_key.starts_with(prefix)
        } else {
            source_key == pattern
        }
    })
}

fn normalize_tag(value: &str) -> Option<String> {
    let normalized = sanitize_for_path(value)
        .to_ascii_lowercase()
        .trim_matches('-')
        .to_string();
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

fn normalize_path_segment(value: &str) -> Option<String> {
    normalize_tag(value)
}

fn add_tag(tags: &mut BTreeSet<String>, value: &str) {
    if let Some(tag) = normalize_tag(value) {
        tags.insert(tag);
    }
}

fn add_path<I, S>(paths: &mut BTreeSet<String>, root: &str, segments: I)
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut parts = vec![root.to_string()];
    for segment in segments {
        if let Some(normalized) = normalize_path_segment(segment.as_ref()) {
            parts.push(normalized);
        }
    }
    if parts.len() > 1 {
        paths.insert(parts.join("/"));
    }
}

fn insert_facet(facets: &mut BTreeMap<String, BTreeSet<String>>, key: &str, value: &str) {
    if let Some(normalized) = normalize_tag(value) {
        facets.entry(key.to_string()).or_default().insert(normalized);
    }
}

fn finalize_facet_map(
    facets: BTreeMap<String, BTreeSet<String>>,
) -> BTreeMap<String, Vec<String>> {
    facets
        .into_iter()
        .map(|(key, values)| (key, values.into_iter().collect::<Vec<_>>()))
        .collect()
}

fn merge_facets(
    target: &mut BTreeMap<String, BTreeSet<String>>,
    source: &BTreeMap<String, Vec<String>>,
) {
    for (key, values) in source {
        let entry = target.entry(key.clone()).or_default();
        for value in values {
            entry.insert(value.clone());
        }
    }
}

fn insert_index_membership(
    index: &mut BTreeMap<String, SnapshotIndexMembership>,
    key: &str,
    event_id: &str,
    calendar_ids: &[String],
) {
    let entry = index.entry(key.to_string()).or_default();
    insert_unique_sorted(&mut entry.event_ids, event_id.to_string());
    extend_unique(&mut entry.calendar_ids, calendar_ids.to_vec());
}

fn insert_optional_scalar_index(
    index: &mut BTreeMap<String, SnapshotIndexMembership>,
    key: Option<&str>,
    event_id: &str,
    calendar_ids: &[String],
) {
    let Some(value) = key.and_then(normalize_tag) else {
        return;
    };
    insert_index_membership(index, &value, event_id, calendar_ids);
}

fn normalize_index_memberships(index: &mut BTreeMap<String, SnapshotIndexMembership>) {
    for membership in index.values_mut() {
        normalize_membership(membership);
    }
}

fn normalize_membership(membership: &mut SnapshotIndexMembership) {
    membership.event_ids.sort();
    membership.event_ids.dedup();
    membership.calendar_ids.sort();
    membership.calendar_ids.dedup();
}

fn extend_unique(target: &mut Vec<String>, values: Vec<String>) {
    for value in values {
        insert_unique_sorted(target, value);
    }
}

fn insert_unique_sorted(target: &mut Vec<String>, value: String) {
    match target.binary_search(&value) {
        Ok(_) => {}
        Err(index) => target.insert(index, value),
    }
}

fn sorted_unique<T: Ord>(mut values: Vec<T>) -> Vec<T> {
    values.sort();
    values.dedup();
    values
}

fn source_ics_filename(
    source: &LoadedSource,
    file_prefix: &str,
    year: i32,
    country: Option<&str>,
) -> String {
    let Some(template) = source.config.publish.file_name_template.as_deref() else {
        return format!("{file_prefix}-{year}.ics");
    };

    let mut file_name = template.to_string();
    file_name = file_name.replace("{{year}}", &year.to_string());
    file_name = file_name.replace("{{source_key}}", &source.config.source.key);
    file_name = file_name.replace("{{source_dir}}", file_prefix);

    if let Some(country) = country {
        file_name = file_name.replace("{{country}}", &country.to_ascii_lowercase());
        file_name = file_name.replace("{{country_upper}}", &country.to_ascii_uppercase());
    } else if let Some(country) = source.config.source.default_country.as_deref() {
        file_name = file_name.replace("{{country}}", &country.to_ascii_lowercase());
        file_name = file_name.replace("{{country_upper}}", &country.to_ascii_uppercase());
    }

    for (key, value) in &source.config.fetch.template_vars {
        file_name = file_name.replace(&format!("{{{{{key}}}}}"), value);
    }

    if file_name.ends_with(".ics") {
        file_name
    } else {
        format!("{file_name}.ics")
    }
}

fn bundle_ics_filename(bundle: &LoadedBundle, file_prefix: &str, year: i32) -> String {
    let Some(template) = bundle.config.publish.file_name_template.as_deref() else {
        return format!("{file_prefix}-{year}.ics");
    };

    let mut file_name = template.to_string();
    file_name = file_name.replace("{{year}}", &year.to_string());
    file_name = file_name.replace("{{bundle_key}}", &bundle.config.bundle.key);
    file_name = file_name.replace("{{bundle_dir}}", file_prefix);

    if file_name.ends_with(".ics") {
        file_name
    } else {
        format!("{file_name}.ics")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn malformed_snapshot_fails_schema_validation() {
        let schema = load_schema(&schema_source_path()).expect("schema should load");
        let value = json!({"schema_version": 1});
        let err = validate_snapshot_value(&schema, &value).expect_err("validation must fail");
        assert!(err.to_string().contains("snapshot json validation failed"));
    }
}
