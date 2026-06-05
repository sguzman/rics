use std::fs;

use anyhow::Result;
use rics::config::{load_bundles_from_dir, load_sources_from_dir};
use rics::pipeline::{SyncOptions, sync_sources};
use tempfile::tempdir;

#[test]
fn top_esports_sources_validate_and_have_expected_keys() -> Result<()> {
    let root = std::env::current_dir()?;
    let sources = load_sources_from_dir(&root.join("configs/sources/esports"))?;
    let bundles = load_bundles_from_dir(&root.join("configs/bundles"))?;

    assert_eq!(sources.len(), 9);
    assert!(bundles
        .iter()
        .any(|bundle| bundle.config.bundle.key == "esports.top_titles"));

    let keys = sources.iter().map(|source| source.config.source.key.as_str()).collect::<Vec<_>>();
    assert!(keys.contains(&"esports.riot.lol"));
    assert!(keys.contains(&"esports.riot.valorant"));
    assert!(keys.contains(&"esports.riot.teamfight_tactics"));
    assert!(keys.contains(&"esports.us_leagues.call_of_duty"));
    assert!(keys.contains(&"esports.us_leagues.overwatch_2"));
    assert!(keys.contains(&"esports.us_leagues.apex_legends"));
    assert!(keys.contains(&"esports.open_circuit.fortnite"));
    assert!(keys.contains(&"esports.open_circuit.rocket_league"));
    assert!(keys.contains(&"esports.open_circuit.ea_sports_fc"));
    Ok(())
}

#[test]
fn top_esports_bundle_builds_from_multiple_titles() -> Result<()> {
    let env = setup_temp_env()?;
    let reports = sync_sources(&SyncOptions {
        config_dir: env.config_dir.clone(),
        state_path: env.state_path.clone(),
        out_dir: env.out_dir.clone(),
        source: None,
        dry_run: false,
    })?;

    assert_eq!(reports.len(), 3);

    let bundle_2026 = env
        .out_dir
        .join("bundles")
        .join("esports-top-titles")
        .join("top-esports-2026.ics");
    assert!(bundle_2026.exists());

    let content = fs::read_to_string(bundle_2026)?;
    assert!(content.contains("SUMMARY:League of Legends Esports: LoL Test Finals"));
    assert!(content.contains("SUMMARY:VALORANT Esports: VALORANT Test Masters"));
    assert!(content.contains("SUMMARY:Call of Duty League: CDL Test Major"));
    Ok(())
}

struct TempEnv {
    config_dir: std::path::PathBuf,
    state_path: std::path::PathBuf,
    out_dir: std::path::PathBuf,
}

fn setup_temp_env() -> Result<TempEnv> {
    let temp_dir = tempdir()?;
    let root = temp_dir.keep();
    let source_dir = root.join("sources");
    let bundle_dir = root.join("bundles");
    let data_dir = root.join("data");
    let state_path = root.join("state.json");
    let out_dir = root.join("out");
    fs::create_dir_all(&source_dir)?;
    fs::create_dir_all(&bundle_dir)?;
    fs::create_dir_all(&data_dir)?;
    fs::create_dir_all(&out_dir)?;

    fs::write(
        source_dir.join("lol.toml"),
        r#"[source]
key = "esports.riot.lol"
name = "League of Legends Esports"
domain = "esports"
enabled = true
timezone = "UTC"

[fetch]
mode = "file"
file_path = "../data/lol.txt"

[extract]
format = "text"

[date]
primary = "date"
formats = ["%Y-%m-%d", "%Y"]
assume_timezone = "UTC"
allow_month_only = true
allow_year_only = true

[event]
event_type = "esports_event"
status = "scheduled"
categories = ["esports", "riot", "league_of_legends"]
importance = 90

[custom]
enabled = true
parser = "structured_calendar_feed_v1"

[publish]
file_name_template = "esports-lol-{{year}}.ics"
"#,
    )?;
    fs::write(
        source_dir.join("valorant.toml"),
        r#"[source]
key = "esports.riot.valorant"
name = "VALORANT Esports"
domain = "esports"
enabled = true
timezone = "UTC"

[fetch]
mode = "file"
file_path = "../data/valorant.txt"

[extract]
format = "text"

[date]
primary = "date"
formats = ["%Y-%m-%d", "%Y"]
assume_timezone = "UTC"
allow_month_only = true
allow_year_only = true

[event]
event_type = "esports_event"
status = "scheduled"
categories = ["esports", "riot", "valorant"]
importance = 90

[custom]
enabled = true
parser = "structured_calendar_feed_v1"

[publish]
file_name_template = "esports-valorant-{{year}}.ics"
"#,
    )?;
    fs::write(
        source_dir.join("cdl.toml"),
        r#"[source]
key = "esports.us_leagues.call_of_duty"
name = "Call of Duty League"
domain = "esports"
enabled = true
timezone = "UTC"

[fetch]
mode = "file"
file_path = "../data/cdl.txt"

[extract]
format = "text"

[date]
primary = "date"
formats = ["%Y-%m-%d", "%Y"]
assume_timezone = "UTC"
allow_month_only = true
allow_year_only = true

[event]
event_type = "esports_event"
status = "scheduled"
categories = ["esports", "us_leagues", "call_of_duty"]
importance = 88

[custom]
enabled = true
parser = "structured_calendar_feed_v1"

[publish]
file_name_template = "esports-call-of-duty-{{year}}.ics"
"#,
    )?;
    fs::write(
        bundle_dir.join("top_esports.toml"),
        r#"[bundle]
key = "esports.top_titles"
name = "Top Esports"

[include]
source_patterns = ["esports.*"]

[publish]
file_name_template = "top-esports-{{year}}.ics"
"#,
    )?;
    fs::write(
        data_dir.join("lol.txt"),
        "2026-10-15 | LoL Test Finals | subtype=world_championship | game=league_of_legends | source_class=official | source_event_id=lol-test-finals-2026 | source_url=https://example.com/lol | description=LoL finals test.\\n",
    )?;
    fs::write(
        data_dir.join("valorant.txt"),
        "2026-06-06 | VALORANT Test Masters | end=2026-06-21 | subtype=international_event | game=valorant | source_class=official | source_event_id=valorant-test-masters-2026 | source_url=https://example.com/valorant | description=VALORANT masters test.\\n",
    )?;
    fs::write(
        data_dir.join("cdl.txt"),
        "2026-07-16 | CDL Test Major | end=2026-07-19 | subtype=major_event | game=call_of_duty | source_class=official | source_event_id=cdl-test-major-2026 | source_url=https://example.com/cdl | description=CDL major test.\\n",
    )?;

    Ok(TempEnv {
        config_dir: source_dir,
        state_path,
        out_dir,
    })
}
