use anyhow::Result;
use rics::config::load_sources_from_dir;
use rics::pipeline::{SyncOptions, sync_sources};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::tempdir;

#[test]
fn wikipedia_film_source_validates_and_has_expected_key() -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let sources = load_sources_from_dir(&root.join("configs/sources/culture"))?;

    let keys = sources
        .into_iter()
        .map(|source| source.config.source.key)
        .collect::<HashSet<_>>();
    assert!(keys.contains("films.us.wikipedia.american"));
    assert!(keys.contains("games.wikipedia.releases"));
    assert!(keys.contains("television.us.wikipedia.debuts"));
    assert!(keys.contains("books.wikipedia.literature"));
    Ok(())
}

#[test]
fn wikipedia_film_parser_builds_calendar_from_api_payload() -> Result<()> {
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
        .join("films-us-wikipedia-american")
        .join("american-films-us-2026.ics");
    assert!(out_file.exists());

    let content = fs::read_to_string(out_file)?;
    assert!(content.contains("SUMMARY:Example Film One"));
    assert!(content.contains("SUMMARY:Example Film Two"));
    assert!(content.contains("DTSTART;VALUE=DATE:20260102"));
    assert!(content.contains("DTSTART;VALUE=DATE:20260109"));

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
        config_dir.join("wikipedia_american_films.toml"),
        r#"[source]
key = "films.us.wikipedia.american"
name = "American Film Releases"
domain = "films"
enabled = true
timezone = "UTC"
jurisdiction = "US"
default_country = "US"

[fetch]
mode = "file"
file_path = "../data/films.json"
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
event_type = "film_release"
status = "scheduled"
categories = ["culture", "films", "wikipedia", "american_films"]
importance = 60

[custom]
enabled = true
parser = "wikipedia_american_films_v1"

[publish]
file_name_template = "american-films-us-{{year}}.ics"
"#,
    )?;

    fs::write(
        data_dir.join("films.json"),
        r#"{
  "parse": {
    "title": "List of American films of 2026",
    "text": "<h2><span id=\"January.E2.80.93March\">January–March</span></h2><table class=\"wikitable\"><tr><th>Opening</th><th>Title</th><th>Production company</th><th>Cast and crew</th><th>Ref.</th></tr><tr><td>J A N U A R Y</td><td>2</td><td><a href=\"/wiki/Example_Film_One\">Example Film One</a></td><td>Example Studio</td><td>Jane Doe (director); Actor One, Actor Two</td><td>[1]</td></tr><tr><td>9</td><td><a href=\"/wiki/Example_Film_Two\">Example Film Two</a></td><td>Another Studio</td><td>John Doe (director); Actor Three</td><td>[2]</td></tr></table>"
  }
}"#,
    )?;

    Ok(TempEnv {
        config_dir,
        state_path: root.join("state.json"),
        out_dir: root.join("out"),
    })
}
