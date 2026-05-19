use anyhow::Result;
use rics::config::{load_bundles_from_dir, load_sources_from_dir};
use rics::model::EventTimeSpec;
use rics::pipeline::{SyncOptions, load_state_for_read, sync_sources};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::tempdir;

#[test]
fn us_state_source_pack_validates_and_has_unique_state_keys() -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let sources = load_sources_from_dir(&root.join("configs/sources/elections/us_states"))?;
    let bundles = load_bundles_from_dir(&root.join("configs/bundles"))?;

    assert!(bundles.iter().any(|bundle| bundle.config.bundle.key == "us_states.elections"));
    assert_eq!(sources.len(), 50);

    let mut keys = HashSet::new();
    let mut states = HashSet::new();
    for source in sources {
        assert!(keys.insert(source.config.source.key.clone()));
        assert!(states.insert(source.config.source.default_country.clone()));
    }

    Ok(())
}

#[test]
fn us_state_shared_feed_filters_events_by_state_and_builds_bundle() -> Result<()> {
    let env = setup_temp_us_state_env()?;

    let reports = sync_sources(&SyncOptions {
        config_dir: env.config_dir.clone(),
        state_path: env.state_path.clone(),
        out_dir: env.out_dir.clone(),
        source: None,
        dry_run: false,
    })?;

    assert_eq!(reports.len(), 2);

    let state = load_state_for_read(&env.state_path)?;
    assert_eq!(state.events.len(), 3);
    assert!(state
        .events
        .values()
        .all(|event| matches!(event.time, EventTimeSpec::Date { .. } | EventTimeSpec::Year { .. })));
    assert!(state
        .events
        .values()
        .all(|event| event.metadata.get("state").is_some()));
    assert!(state
        .events
        .values()
        .filter(|event| event.country.as_deref() == Some("TX"))
        .all(|event| event.metadata.get("state").map(String::as_str) == Some("TX")));

    let texas_2026 = env
        .out_dir
        .join("sources")
        .join("elections-us-tx")
        .join("us-elections-tx-2026.ics");
    assert!(texas_2026.exists());

    let bundle_2026 = env
        .out_dir
        .join("bundles")
        .join("us-states-elections")
        .join("us-state-elections-2026.ics");
    assert!(bundle_2026.exists());

    let content = fs::read_to_string(bundle_2026)?;
    assert!(content.contains("SUMMARY:Texas: Governor election"));
    assert!(content.contains("SUMMARY:California: State Senate election"));
    assert!(!content.contains("SUMMARY:California: Governor election"));

    Ok(())
}

struct TempUsStateEnv {
    config_dir: PathBuf,
    state_path: PathBuf,
    out_dir: PathBuf,
}

fn setup_temp_us_state_env() -> Result<TempUsStateEnv> {
    let temp = tempdir()?;
    let root = temp.keep();

    let config_dir = root.join("sources");
    let bundle_dir = root.join("bundles");
    let data_dir = root.join("data");
    fs::create_dir_all(&config_dir)?;
    fs::create_dir_all(&bundle_dir)?;
    fs::create_dir_all(&data_dir)?;

    fs::write(
        config_dir.join("tx.toml"),
        r#"[source]
key = "elections.us.tx"
name = "Texas"
domain = "elections"
enabled = true
timezone = "UTC"
jurisdiction = "TX"
default_country = "TX"

[fetch]
mode = "file"
file_path = "../data/us_states.txt"
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
categories = ["elections", "us_states", "united_states"]
importance = 80

[custom]
enabled = true
parser = "us_state_elections_feed_v1"

[publish]
file_name_template = "us-elections-{{country}}-{{year}}.ics"
"#,
    )?;

    fs::write(
        config_dir.join("ca.toml"),
        r#"[source]
key = "elections.us.ca"
name = "California"
domain = "elections"
enabled = true
timezone = "UTC"
jurisdiction = "CA"
default_country = "CA"

[fetch]
mode = "file"
file_path = "../data/us_states.txt"
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
categories = ["elections", "us_states", "united_states"]
importance = 80

[custom]
enabled = true
parser = "us_state_elections_feed_v1"

[publish]
file_name_template = "us-elections-{{country}}-{{year}}.ics"
"#,
    )?;

    fs::write(
        bundle_dir.join("us_states.toml"),
        r#"[bundle]
key = "us_states.elections"
name = "US State Elections"

[include]
source_patterns = ["elections.us.*"]

[publish]
file_name_template = "us-state-elections-{{year}}.ics"
"#,
    )?;

    fs::write(
        data_dir.join("us_states.txt"),
        concat!(
            "2026-11-03 | Governor election | state=TX | subtype=governor | office=governor | ",
            "election_kind=general | source_class=aggregator | source_event_id=us-tx-2026-governor-general | ",
            "source_url=https://www.nga.org/governors/elections/ | description=Texas elects its governor in the 2026 general election cycle.\n",
            "2026-11-03 | State Senate election | state=CA | subtype=state_legislature_upper | office=state_senate | ",
            "election_kind=general | source_class=aggregator | source_event_id=us-ca-2026-state-senate-general | ",
            "source_url=https://www.ncsl.org/elections-and-campaigns/ncsl-state-elections-2026 | description=California elects State Senate members in the 2026 general election.\n",
            "2028 | Governor election | state=CA | subtype=governor | office=governor | election_kind=general | ",
            "source_class=aggregator | source_event_id=us-ca-2028-governor-general | ",
            "source_url=https://www.nga.org/governors/elections/ | description=California next elects its governor in 2028.\n",
        ),
    )?;

    Ok(TempUsStateEnv {
        config_dir,
        state_path: root.join("state.json"),
        out_dir: root.join("out"),
    })
}
