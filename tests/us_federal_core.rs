use anyhow::Result;
use rics::config::{load_bundles_from_dir, load_sources_from_dir};
use rics::model::EventTimeSpec;
use rics::pipeline::{SyncOptions, load_state_for_read, sync_sources};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::tempdir;

#[test]
fn us_federal_source_pack_validates_and_has_expected_keys() -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let sources = load_sources_from_dir(&root.join("configs/sources/federal/us_core"))?;
    let bundles = load_bundles_from_dir(&root.join("configs/bundles"))?;

    assert!(bundles.iter().any(|bundle| bundle.config.bundle.key == "us_federal.core"));
    assert_eq!(sources.len(), 5);

    let mut keys = HashSet::new();
    for source in sources {
        assert!(keys.insert(source.config.source.key.clone()));
        assert_eq!(source.config.source.default_country.as_deref(), Some("US"));
    }

    Ok(())
}

#[test]
fn us_federal_bundle_builds_from_multiple_sources() -> Result<()> {
    let env = setup_temp_federal_env()?;

    let reports = sync_sources(&SyncOptions {
        config_dir: env.config_dir.clone(),
        state_path: env.state_path.clone(),
        out_dir: env.out_dir.clone(),
        source: None,
        dry_run: false,
    })?;

    assert_eq!(reports.len(), 2);

    let state = load_state_for_read(&env.state_path)?;
    assert!(state.events.len() >= 2);
    assert!(state.events.values().any(|event| matches!(event.time, EventTimeSpec::Date { .. })));

    let bundle_2026 = env
        .out_dir
        .join("bundles")
        .join("us-federal-core")
        .join("us-federal-core-2026.ics");
    assert!(bundle_2026.exists());

    let content = fs::read_to_string(bundle_2026)?;
    assert!(content.contains("SUMMARY:US Federal Elections: Federal general election"));
    assert!(content.contains("SUMMARY:US Supreme Court: January oral argument session begins"));

    Ok(())
}

struct TempFederalEnv {
    config_dir: PathBuf,
    state_path: PathBuf,
    out_dir: PathBuf,
}

fn setup_temp_federal_env() -> Result<TempFederalEnv> {
    let temp = tempdir()?;
    let root = temp.keep();

    let config_dir = root.join("sources");
    let bundle_dir = root.join("bundles");
    let data_dir = root.join("data");
    fs::create_dir_all(&config_dir)?;
    fs::create_dir_all(&bundle_dir)?;
    fs::create_dir_all(&data_dir)?;

    fs::write(
        config_dir.join("elections.toml"),
        r#"[source]
key = "federal.us.elections"
name = "US Federal Elections"
domain = "federal"
enabled = true
timezone = "UTC"
jurisdiction = "US"
default_country = "US"

[fetch]
mode = "file"
file_path = "../data/elections.txt"
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
event_type = "federal_event"
status = "scheduled"
categories = ["federal", "united_states", "elections"]
importance = 80

[custom]
enabled = true
parser = "structured_calendar_feed_v1"

[publish]
file_name_template = "us-federal-elections-{{year}}.ics"
"#,
    )?;

    fs::write(
        config_dir.join("scotus.toml"),
        r#"[source]
key = "federal.us.scotus"
name = "US Supreme Court"
domain = "federal"
enabled = true
timezone = "UTC"
jurisdiction = "US"
default_country = "US"

[fetch]
mode = "file"
file_path = "../data/scotus.txt"
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
event_type = "federal_event"
status = "scheduled"
categories = ["federal", "united_states", "judicial"]
importance = 80

[custom]
enabled = true
parser = "structured_calendar_feed_v1"

[publish]
file_name_template = "us-federal-scotus-{{year}}.ics"
"#,
    )?;

    fs::write(
        bundle_dir.join("us_federal.toml"),
        r#"[bundle]
key = "us_federal.core"
name = "US Federal Core"

[include]
source_patterns = ["federal.us.*"]

[publish]
file_name_template = "us-federal-core-{{year}}.ics"
"#,
    )?;

    fs::write(
        data_dir.join("elections.txt"),
        concat!(
            "2026-11-03 | Federal general election | subtype=federal_general_election | office=presidency_house_senate | source_class=official | source_event_id=test-fed-general | source_url=https://www.fec.gov/introduction-campaign-finance/election-results-and-voting-information/ | description=Federal general election day.\\n",
            "2026-10-22 | FEC pre-general report due | subtype=campaign_finance_deadline | office=fec_reporting | source_class=official | source_event_id=test-fed-preg | source_url=https://www.fec.gov/help-candidates-and-committees/dates-and-deadlines/2026-reporting-dates/pre-and-post-general-reports-2026/ | description=Pre-general report deadline.\\n",
        ),
    )?;

    fs::write(
        data_dir.join("scotus.txt"),
        concat!(
            "2026-01-12 | January oral argument session begins | subtype=oral_arguments | office=supreme_court | source_class=official | source_event_id=test-scotus-jan | source_url=https://www.supremecourt.gov/oral_arguments/calendarsandlists.aspx?os=app | description=January session.\\n",
            "October 2026 | Supreme Court calendar for October Term 2026 | subtype=term_calendar | office=supreme_court | source_class=official | source_event_id=test-scotus-ot2026 | source_url=https://www.supremecourt.gov/oral_arguments/calendarsandlists.aspx?os=app | description=OT2026 calendar.\\n",
        ),
    )?;

    Ok(TempFederalEnv {
        config_dir,
        state_path: root.join("state.json"),
        out_dir: root.join("out"),
    })
}
