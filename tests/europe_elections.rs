use anyhow::Result;
use rics::config::{load_bundles_from_dir, load_sources_from_dir};
use rics::model::EventTimeSpec;
use rics::pipeline::{SyncOptions, load_state_for_read, sync_sources};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::tempdir;

#[test]
fn europe_source_pack_validates_and_has_unique_country_keys() -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let sources = load_sources_from_dir(&root.join("configs/sources/elections/europe"))?;
    let bundles = load_bundles_from_dir(&root.join("configs/bundles"))?;

    assert_eq!(bundles.len(), 1);
    assert_eq!(bundles[0].config.bundle.key, "europe.elections");
    assert!(sources.len() >= 45);

    let mut keys = HashSet::new();
    let mut countries = HashSet::new();
    for source in sources {
        assert!(keys.insert(source.config.source.key.clone()));
        assert!(countries.insert(source.config.source.default_country.clone()));
    }

    Ok(())
}

#[test]
fn parser_supports_exact_month_year_and_tbd_and_builds_bundle() -> Result<()> {
    let env = setup_temp_election_env()?;

    let reports = sync_sources(&SyncOptions {
        config_dir: env.config_dir.clone(),
        state_path: env.state_path.clone(),
        out_dir: env.out_dir.clone(),
        source: None,
        dry_run: false,
    })?;

    assert_eq!(reports.len(), 2);

    let state = load_state_for_read(&env.state_path)?;
    assert_eq!(state.events.len(), 5);

    let alpha_exact = state
        .events
        .values()
        .find(|event| event.source_event_id.as_deref() == Some("aa-exact"))
        .expect("exact event must exist");
    assert!(matches!(alpha_exact.time, EventTimeSpec::Date { .. }));

    let alpha_month = state
        .events
        .values()
        .find(|event| event.source_event_id.as_deref() == Some("aa-month"))
        .expect("month event must exist");
    assert!(matches!(alpha_month.time, EventTimeSpec::Month { .. }));

    let alpha_year = state
        .events
        .values()
        .find(|event| event.source_event_id.as_deref() == Some("aa-year"))
        .expect("year event must exist");
    assert!(matches!(alpha_year.time, EventTimeSpec::Year { .. }));

    let alpha_tbd = state
        .events
        .values()
        .find(|event| event.source_event_id.as_deref() == Some("aa-tbd"))
        .expect("tbd event must exist");
    assert!(matches!(alpha_tbd.time, EventTimeSpec::Tbd { .. }));

    let beta = state
        .events
        .values()
        .find(|event| event.source_event_id.as_deref() == Some("bb-runoff"))
        .expect("runoff event must exist");
    assert_eq!(
        beta.metadata.get("official_url").map(String::as_str),
        Some("https://official.example/bb")
    );

    let bundle_2026 = env
        .out_dir
        .join("bundles")
        .join("europe-elections")
        .join("europe-elections-2026.ics");
    assert!(bundle_2026.exists());

    let content = fs::read_to_string(bundle_2026)?;
    assert!(content.contains("SUMMARY:Alpha Republic: Parliamentary election"));
    assert!(content.contains("SUMMARY:Beta Republic: Presidential election runoff"));
    assert!(!content.contains("STATUS:CANCELLED"));

    Ok(())
}

#[test]
fn election_updates_increment_sequence() -> Result<()> {
    let env = setup_temp_election_env()?;

    sync_sources(&SyncOptions {
        config_dir: env.config_dir.clone(),
        state_path: env.state_path.clone(),
        out_dir: env.out_dir.clone(),
        source: None,
        dry_run: false,
    })?;

    let file = env.data_dir.join("aa.txt");
    let original = fs::read_to_string(&file)?;
    let updated = original.replace("2026-04-12", "2026-04-19");
    fs::write(&file, updated)?;

    let reports = sync_sources(&SyncOptions {
        config_dir: env.config_dir.clone(),
        state_path: env.state_path.clone(),
        out_dir: env.out_dir.clone(),
        source: Some("elections.aa".to_string()),
        dry_run: false,
    })?;

    assert_eq!(reports[0].updated, 1);

    let state = load_state_for_read(&env.state_path)?;
    let record = state
        .events
        .values()
        .find(|event| event.source_event_id.as_deref() == Some("aa-exact"))
        .expect("updated record must exist");
    assert_eq!(record.sequence, 1);

    Ok(())
}

struct TempElectionEnv {
    config_dir: PathBuf,
    data_dir: PathBuf,
    state_path: PathBuf,
    out_dir: PathBuf,
}

fn setup_temp_election_env() -> Result<TempElectionEnv> {
    let temp = tempdir()?;
    let root = temp.keep();

    let config_dir = root.join("sources");
    let bundle_dir = root.join("bundles");
    let data_dir = root.join("data");
    fs::create_dir_all(&config_dir)?;
    fs::create_dir_all(&bundle_dir)?;
    fs::create_dir_all(&data_dir)?;

    fs::write(
        config_dir.join("aa.toml"),
        r#"[source]
key = "elections.aa"
name = "Alpha Republic"
domain = "elections"
enabled = true
timezone = "UTC"
jurisdiction = "AA"
default_country = "AA"

[fetch]
mode = "file"
file_path = "../data/aa.txt"
timeout_secs = 10
retry_attempts = 1
retry_backoff_ms = 10

[extract]
format = "text"

[date]
primary = "date"
formats = ["%Y-%m-%d", "%Y"]
assume_timezone = "UTC"
allow_month_only = true
allow_year_only = true

[event]
event_type = "election"
status = "scheduled"
categories = ["elections", "europe"]
importance = 80

[custom]
enabled = true
parser = "europe_elections_feed_v1"

[publish]
file_name_template = "elections-{{country}}-{{year}}.ics"
"#,
    )?;

    fs::write(
        config_dir.join("bb.toml"),
        r#"[source]
key = "elections.bb"
name = "Beta Republic"
domain = "elections"
enabled = true
timezone = "UTC"
jurisdiction = "BB"
default_country = "BB"

[fetch]
mode = "file"
file_path = "../data/bb.txt"
timeout_secs = 10
retry_attempts = 1
retry_backoff_ms = 10

[extract]
format = "text"

[date]
primary = "date"
formats = ["%Y-%m-%d", "%Y"]
assume_timezone = "UTC"
allow_month_only = true
allow_year_only = true

[event]
event_type = "election"
status = "scheduled"
categories = ["elections", "europe"]
importance = 80

[custom]
enabled = true
parser = "europe_elections_feed_v1"

[publish]
file_name_template = "elections-{{country}}-{{year}}.ics"
"#,
    )?;

    fs::write(
        bundle_dir.join("europe.toml"),
        r#"[bundle]
key = "europe.elections"
name = "European Elections"

[include]
source_patterns = ["elections.*"]

[publish]
file_name_template = "europe-elections-{{year}}.ics"
"#,
    )?;

    fs::write(
        data_dir.join("aa.txt"),
        concat!(
            "2026-04-12 | Parliamentary election | subtype=parliamentary | office=parliament | ",
            "election_kind=parliamentary | source_class=official | source_event_id=aa-exact | ",
            "official_url=https://official.example/aa\n",
            "2026-06 | Presidential election window | subtype=presidential | office=president | ",
            "election_kind=presidential | source_class=aggregator | source_event_id=aa-month | ",
            "source_url=https://aggregator.example/aa\n",
            "2027 | National referendum | subtype=referendum | office=referendum | ",
            "election_kind=referendum | source_class=aggregator | source_event_id=aa-year | ",
            "source_url=https://aggregator.example/aa-referendum\n",
            "TBD | Constitutional referendum | subtype=referendum | office=referendum | ",
            "election_kind=referendum | source_class=aggregator | source_event_id=aa-tbd | ",
            "tbd=Date to be announced\n"
        ),
    )?;

    fs::write(
        data_dir.join("bb.txt"),
        concat!(
            "2026-05-01 | Presidential election runoff | subtype=presidential | office=president | ",
            "round=2 | election_kind=presidential | source_class=official | source_event_id=bb-runoff | ",
            "official_url=https://official.example/bb\n"
        ),
    )?;

    Ok(TempElectionEnv {
        config_dir,
        data_dir,
        state_path: root.join("state/events.json"),
        out_dir: root.join("out"),
    })
}
