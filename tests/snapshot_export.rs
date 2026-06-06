use anyhow::Result;
use rics::pipeline::{BuildOptions, SyncOptions, build_calendars, sync_sources};
use serde_json::Value;
use std::fs;
use std::path::PathBuf;
use tempfile::tempdir;

#[test]
fn sync_and_build_write_snapshot_with_split_country_and_bundle_views() -> Result<()> {
    let env = setup_temp_snapshot_env()?;

    sync_sources(&SyncOptions {
        config_dir: env.config_dir.clone(),
        state_path: env.state_path.clone(),
        out_dir: env.out_dir.clone(),
        source: None,
        dry_run: false,
    })?;

    let snapshot_path = env.out_dir.join("snapshots").join("rics-all.json");
    let schema_path = env.out_dir.join("snapshots").join("rics-all.schema.json");
    assert!(snapshot_path.exists());
    assert!(schema_path.exists());

    let pretty = fs::read_to_string(&snapshot_path)?;
    assert!(pretty.starts_with("{\n  \"schema_version\""));

    let snapshot: Value = serde_json::from_str(&pretty)?;
    assert!(
        snapshot["calendars"]
            .get("source:test.econ:country:us")
            .is_some()
    );
    assert!(
        snapshot["calendars"]
            .get("source:test.econ:country:ca")
            .is_some()
    );
    assert!(snapshot["calendars"].get("source:test.sports").is_some());
    assert!(snapshot["calendars"].get("bundle:test.all").is_some());
    assert!(snapshot["indexes"]["years"].get("2026").is_some());
    assert!(snapshot["indexes"]["tags"].get("sports").is_some());
    assert!(
        snapshot["indexes"]["ontology_paths"]
            .get("domain/economic-indicators")
            .is_some()
    );
    assert_eq!(
        snapshot["events"].as_object().expect("events object").len(),
        3
    );

    fs::remove_dir_all(env.out_dir.join("snapshots"))?;

    build_calendars(&BuildOptions {
        config_dir: env.config_dir.clone(),
        state_path: env.state_path.clone(),
        out_dir: env.out_dir.clone(),
        source: None,
        year: None,
    })?;

    assert!(snapshot_path.exists());
    assert!(schema_path.exists());
    Ok(())
}

struct TempSnapshotEnv {
    config_dir: PathBuf,
    state_path: PathBuf,
    out_dir: PathBuf,
}

fn setup_temp_snapshot_env() -> Result<TempSnapshotEnv> {
    let temp = tempdir()?;
    let root = temp.keep();

    let config_dir = root.join("sources");
    let bundle_dir = root.join("bundles");
    let data_dir = root.join("data");
    fs::create_dir_all(&config_dir)?;
    fs::create_dir_all(&bundle_dir)?;
    fs::create_dir_all(&data_dir)?;

    fs::write(
        config_dir.join("econ.toml"),
        r#"[source]
key = "test.econ"
name = "Test Economic Calendar"
domain = "economic-indicators"
enabled = true
timezone = "America/New_York"
jurisdiction = "GLOBAL"

[fetch]
mode = "file"
file_path = "../data/econ.txt"
timeout_secs = 10
retry_attempts = 1
retry_backoff_ms = 10

[extract]
format = "text"

[date]
primary = "start"
formats = ["%Y-%m-%d %H:%M"]
assume_timezone = "America/New_York"
allow_month_only = false
allow_year_only = false

[event]
event_type = "economic_indicator"
subtype = "macro_release"
status = "scheduled"
categories = ["economic-indicators", "macro"]
importance = 70

[custom]
enabled = true
parser = "econ_indicators_calendar_v1"

[publish]
file_name_template = "econ-{{country}}-{{year}}.ics"
split_by_country = true
"#,
    )?;

    fs::write(
        config_dir.join("sports.toml"),
        r#"[source]
key = "test.sports"
name = "Test Sports"
domain = "sports"
enabled = true
timezone = "UTC"
jurisdiction = "US"
default_country = "US"

[fetch]
mode = "file"
file_path = "../data/sports.txt"
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
event_type = "sports_event"
status = "scheduled"
categories = ["sports", "test"]
importance = 75

[custom]
enabled = true
parser = "structured_calendar_feed_v1"

[publish]
file_name_template = "test-sports-{{year}}.ics"
"#,
    )?;

    fs::write(
        bundle_dir.join("test_bundle.toml"),
        r#"[bundle]
key = "test.all"
name = "Test Aggregate"

[include]
source_patterns = ["test.*"]

[publish]
file_name_template = "test-all-{{year}}.ics"
"#,
    )?;

    fs::write(
        data_dir.join("econ.txt"),
        concat!(
            "UTC -5\n\n",
            "Monday January 05 2026\tActual\tPrevious\tConsensus\tForecast\n",
            "08:30 AM\n",
            "US\n",
            "CPI YoY DEC\t2.9%\t2.7%\t2.8%\t2.8%\n",
            "09:00 AM\n",
            "CA\n",
            "GDP MoM NOV\t0.2%\t0.1%\t0.2%\t0.2%\n"
        ),
    )?;

    fs::write(
        data_dir.join("sports.txt"),
        "2026-02-05 | Test Sports Deadline | subtype=trade_deadline | league=nba | source_class=official | source_event_id=test-sports-deadline | source_url=https://example.test/sports | description=Deadline.\n",
    )?;

    Ok(TempSnapshotEnv {
        config_dir,
        state_path: root.join("state.json"),
        out_dir: root.join("out"),
    })
}
