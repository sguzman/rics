use anyhow::Result;
use rics::config::load_sources_from_dir;
use rics::pipeline::{SyncOptions, sync_sources};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::tempdir;

#[test]
fn wikipedia_video_game_source_validates_and_has_expected_key() -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let sources = load_sources_from_dir(&root.join("configs/sources/culture"))?;

    let keys = sources
        .into_iter()
        .map(|source| source.config.source.key)
        .collect::<HashSet<_>>();
    assert!(keys.contains("games.wikipedia.releases"));
    Ok(())
}

#[test]
fn wikipedia_video_game_parser_builds_calendar_from_api_payload() -> Result<()> {
    let env = setup_temp_env()?;

    let reports = sync_sources(&SyncOptions {
        config_dir: env.config_dir.clone(),
        state_path: env.state_path.clone(),
        out_dir: env.out_dir.clone(),
        source: None,
        dry_run: false,
    })?;

    assert_eq!(reports.len(), 1);

    let out_file = env
        .out_dir
        .join("sources")
        .join("games-wikipedia-releases")
        .join("video-games-2026.ics");
    assert!(out_file.exists());

    let content = fs::read_to_string(out_file)?;
    assert!(content.contains("SUMMARY:Example Game One"));
    assert!(content.contains("SUMMARY:Example Game Two"));
    assert!(content.contains("DTSTART;VALUE=DATE:20260105"));
    assert!(content.contains("DTSTART;VALUE=DATE:20260402"));
    assert!(content.contains("X-RICS-DEVELOPER-S:Studio One"));
    assert!(content.contains("X-RICS-WIKIPEDIA-SCHEMA:video_games"));

    Ok(())
}

struct TempEnv {
    config_dir: PathBuf,
    state_path: PathBuf,
    out_dir: PathBuf,
}

fn setup_temp_env() -> Result<TempEnv> {
    let temp = tempdir()?;
    let root = temp.keep();
    let config_dir = root.join("sources");
    let data_dir = root.join("data");
    fs::create_dir_all(&config_dir)?;
    fs::create_dir_all(&data_dir)?;

    fs::write(
        config_dir.join("wikipedia_video_games.toml"),
        r#"[source]
key = "games.wikipedia.releases"
name = "Video Game Releases"
domain = "games"
enabled = true
timezone = "UTC"
jurisdiction = "GLOBAL"

[fetch]
mode = "file"
file_path = "../data/games.json"
timeout_secs = 10
retry_attempts = 1
retry_backoff_ms = 10

[extract]
format = "json"

[date]
primary = "date"
formats = ["%Y-%m-%d", "%Y"]
assume_timezone = "UTC"
allow_month_only = true
allow_year_only = true

[event]
event_type = "video_game_release"
status = "scheduled"
categories = ["culture", "games", "wikipedia", "video_games"]
importance = 60

[custom]
enabled = true
parser = "wikipedia_release_table_v1"

[publish]
file_name_template = "video-games-{{year}}.ics"
"#,
    )?;

    fs::write(
        data_dir.join("games.json"),
        r#"{
  "parse": {
    "title": "List of video games released in 2026",
    "text": "<h2><span id=\"January.E2.80.93March\">January–March</span></h2><table class=\"wikitable\"><tr><th>Release date</th><th>Title</th><th>Platform(s)</th><th>Type(s)</th><th>Genre(s)</th><th>Developer(s)</th><th>Publisher(s)</th><th>Ref.</th></tr><tr><td>January 5</td><td><a href=\"/wiki/Example_Game_One\">Example Game One</a></td><td>WIN, PS5</td><td>Original</td><td>RPG</td><td>Studio One</td><td>Publisher One</td><td>[1]</td></tr><tr><td>April 2</td><td><a href=\"/wiki/Example_Game_Two\">Example Game Two</a></td><td>NS2</td><td>Port</td><td>Action-adventure</td><td>Studio Two</td><td>Publisher Two</td><td>[2]</td></tr></table>"
  }
}"#,
    )?;

    Ok(TempEnv {
        config_dir,
        state_path: root.join("state.json"),
        out_dir: root.join("out"),
    })
}
