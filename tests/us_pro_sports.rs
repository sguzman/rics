use anyhow::Result;
use rics::config::{load_bundles_from_dir, load_sources_from_dir};
use rics::pipeline::{SyncOptions, sync_sources};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::tempdir;

#[test]
fn us_pro_sports_sources_validate_and_have_expected_keys() -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let sources = load_sources_from_dir(&root.join("configs/sources/sports/us_pro"))?;
    let bundles = load_bundles_from_dir(&root.join("configs/bundles"))?;

    assert_eq!(sources.len(), 10);
    assert!(bundles
        .iter()
        .any(|bundle| bundle.config.bundle.key == "us_pro_sports.all"));

    let keys = sources
        .into_iter()
        .map(|source| source.config.source.key)
        .collect::<HashSet<_>>();
    assert!(keys.contains("sports.us.nfl"));
    assert!(keys.contains("sports.us.nba"));
    assert!(keys.contains("sports.us.mlb"));
    assert!(keys.contains("sports.us.mlb.schedule"));
    assert!(keys.contains("sports.us.nfl.schedule"));
    assert!(keys.contains("sports.us.nhl"));
    assert!(keys.contains("sports.us.nhl.schedule"));
    assert!(keys.contains("sports.us.mls"));
    assert!(keys.contains("sports.us.nwsl"));
    assert!(keys.contains("sports.us.nba.schedule"));

    Ok(())
}

#[test]
fn us_pro_sports_bundle_builds_from_multiple_leagues() -> Result<()> {
    let env = setup_temp_sports_env()?;

    let reports = sync_sources(&SyncOptions {
        config_dir: env.config_dir.clone(),
        state_path: env.state_path.clone(),
        out_dir: env.out_dir.clone(),
        source: None,
        dry_run: false,
    })?;

    assert_eq!(reports.len(), 2);

    let bundle_2026 = env
        .out_dir
        .join("bundles")
        .join("us-pro-sports-all")
        .join("us-pro-sports-2026.ics");
    assert!(bundle_2026.exists());

    let content = fs::read_to_string(bundle_2026)?;
    assert!(content.contains("SUMMARY:NFL: Schedule release"));
    assert!(content.contains("SUMMARY:NBA: Trade deadline"));

    Ok(())
}

struct TempSportsEnv {
    config_dir: PathBuf,
    state_path: PathBuf,
    out_dir: PathBuf,
}

fn setup_temp_sports_env() -> Result<TempSportsEnv> {
    let temp = tempdir()?;
    let root = temp.keep();

    let config_dir = root.join("sources");
    let bundle_dir = root.join("bundles");
    let data_dir = root.join("data");
    fs::create_dir_all(&config_dir)?;
    fs::create_dir_all(&bundle_dir)?;
    fs::create_dir_all(&data_dir)?;

    fs::write(
        config_dir.join("nfl.toml"),
        r#"[source]
key = "sports.us.nfl"
name = "NFL"
domain = "sports"
enabled = true
timezone = "UTC"
jurisdiction = "US"
default_country = "US"

[fetch]
mode = "file"
file_path = "../data/nfl.txt"
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
categories = ["sports", "us_pro", "nfl"]
importance = 80

[custom]
enabled = true
parser = "structured_calendar_feed_v1"

[publish]
file_name_template = "us-sports-nfl-{{year}}.ics"
"#,
    )?;

    fs::write(
        config_dir.join("nba.toml"),
        r#"[source]
key = "sports.us.nba"
name = "NBA"
domain = "sports"
enabled = true
timezone = "UTC"
jurisdiction = "US"
default_country = "US"

[fetch]
mode = "file"
file_path = "../data/nba.txt"
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
categories = ["sports", "us_pro", "nba"]
importance = 80

[custom]
enabled = true
parser = "structured_calendar_feed_v1"

[publish]
file_name_template = "us-sports-nba-{{year}}.ics"
"#,
    )?;

    fs::write(
        bundle_dir.join("us_pro_sports.toml"),
        r#"[bundle]
key = "us_pro_sports.all"
name = "US Pro Sports"

[include]
source_patterns = ["sports.us.*"]

[publish]
file_name_template = "us-pro-sports-{{year}}.ics"
"#,
    )?;

    fs::write(
        data_dir.join("nfl.txt"),
        concat!(
            "2026-05-14 | NFL: Schedule release | subtype=special_event | league=nfl | source_class=official | source_event_id=test-nfl-release | source_url=https://operations.nfl.com/updates/the-game/2026-nfl-schedule-announced/ | description=Schedule release.\\n",
        ),
    )?;

    fs::write(
        data_dir.join("nba.txt"),
        concat!(
            "2026-02-05 | NBA: Trade deadline | subtype=trade_deadline | league=nba | source_class=official | source_event_id=test-nba-deadline | source_url=https://www.nba.com/news/key-dates?os=w | description=Trade deadline.\\n",
        ),
    )?;

    Ok(TempSportsEnv {
        config_dir,
        state_path: root.join("state.json"),
        out_dir: root.join("out"),
    })
}
