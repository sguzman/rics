use anyhow::Result;
use rics::config::{load_bundles_from_dir, load_sources_from_dir};
use rics::pipeline::{SyncOptions, sync_sources};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::tempdir;

#[test]
fn wikipedia_culture_sources_and_bundle_validate() -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let sources = load_sources_from_dir(&root.join("configs/sources/culture"))?;
    let bundles = load_bundles_from_dir(&root.join("configs/bundles"))?;

    let keys = sources
        .into_iter()
        .map(|source| source.config.source.key)
        .collect::<HashSet<_>>();
    assert!(keys.contains("anime.wikipedia.television"));
    assert!(keys.contains("television.us.wikipedia.debuts"));
    assert!(keys.contains("television.gb.wikipedia.debuts"));
    assert!(keys.contains("books.wikipedia.literature"));
    assert!(
        bundles
            .iter()
            .any(|bundle| bundle.config.bundle.key == "culture.releases")
    );

    Ok(())
}

#[test]
fn wikipedia_television_and_literature_build_calendars_and_bundle() -> Result<()> {
    let env = setup_temp_env()?;

    let reports = sync_sources(&SyncOptions {
        config_dir: env.config_dir.clone(),
        state_path: env.state_path.clone(),
        out_dir: env.out_dir.clone(),
        source: None,
        dry_run: false,
    })?;

    assert_eq!(reports.len(), 2);

    let tv_file = env
        .out_dir
        .join("sources")
        .join("television-us-wikipedia-debuts")
        .join("american-television-us-2026.ics");
    let books_file = env
        .out_dir
        .join("sources")
        .join("books-wikipedia-literature")
        .join("literature-releases-2026.ics");
    let bundle_file = env
        .out_dir
        .join("bundles")
        .join("culture-releases")
        .join("culture-releases-2026.ics");

    assert!(tv_file.exists());
    assert!(books_file.exists());
    assert!(bundle_file.exists());

    let tv = fs::read_to_string(tv_file)?;
    assert!(tv.contains("SUMMARY:Example Show One"));
    assert!(tv.contains("SUMMARY:Example Show Two"));
    assert!(tv.contains("DTSTART;VALUE=DATE:20260105"));
    assert!(tv.contains("DTSTART;VALUE=DATE:20260401"));
    assert!(tv.contains("X-RICS-CHANNEL:Example TV"));
    assert!(tv.contains("X-RICS-WIKIPEDIA-SCHEMA:american_television"));

    let books = fs::read_to_string(books_file)?;
    assert!(books.contains("SUMMARY:Example Book One"));
    assert!(books.contains("SUMMARY:Example Book Two"));
    assert!(books.contains("DTSTART;VALUE=DATE:20260106"));
    assert!(books.contains("X-RICS-AUTHOR:Author One"));
    assert!(books.contains("X-RICS-PUBLICATION-NOTE:UK"));

    let bundle = fs::read_to_string(bundle_file)?;
    assert!(bundle.contains("SUMMARY:Example Show One"));
    assert!(bundle.contains("SUMMARY:Example Book One"));

    Ok(())
}

#[test]
fn wikipedia_anime_and_british_television_build_calendars() -> Result<()> {
    let env = setup_more_temp_env()?;

    let reports = sync_sources(&SyncOptions {
        config_dir: env.config_dir.clone(),
        state_path: env.state_path.clone(),
        out_dir: env.out_dir.clone(),
        source: None,
        dry_run: false,
    })?;

    assert_eq!(reports.len(), 2);

    let anime_file = env
        .out_dir
        .join("sources")
        .join("anime-wikipedia-television")
        .join("anime-television-2026.ics");
    let british_tv_file = env
        .out_dir
        .join("sources")
        .join("television-gb-wikipedia-debuts")
        .join("british-television-2026.ics");

    assert!(anime_file.exists());
    assert!(british_tv_file.exists());

    let anime = fs::read_to_string(anime_file)?;
    assert!(anime.contains("SUMMARY:Example Anime One"));
    assert!(anime.contains("DTSTART;VALUE=DATE:20260103"));
    assert!(anime.contains("X-RICS-STUDIO:Studio One"));
    assert!(anime.contains("X-RICS-WIKIPEDIA-SCHEMA:anime_television"));

    let british_tv = fs::read_to_string(british_tv_file)?;
    assert!(british_tv.contains("SUMMARY:Example UK Show One"));
    assert!(british_tv.contains("SUMMARY:Example UK Show Two"));
    assert!(british_tv.contains("DTSTART;VALUE=DATE:20260101"));
    assert!(british_tv.contains("X-RICS-CHANNEL:BBC One"));
    assert!(british_tv.contains("X-RICS-WIKIPEDIA-SCHEMA:british_television"));

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
    let source_dir = root.join("sources").join("culture");
    let bundle_dir = root.join("bundles");
    let data_dir = root.join("data");
    fs::create_dir_all(&source_dir)?;
    fs::create_dir_all(&bundle_dir)?;
    fs::create_dir_all(&data_dir)?;

    fs::write(
        source_dir.join("wikipedia_american_television.toml"),
        r#"[source]
key = "television.us.wikipedia.debuts"
name = "American Television Debuts"
domain = "television"
enabled = true
timezone = "UTC"
jurisdiction = "US"
default_country = "US"

[fetch]
mode = "file"
file_path = "../../data/television.json"
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
event_type = "television_release"
status = "scheduled"
categories = ["culture", "television", "wikipedia", "american_television"]
importance = 55

[custom]
enabled = true
parser = "wikipedia_release_table_v1"

[publish]
file_name_template = "american-television-us-{{year}}.ics"
"#,
    )?;

    fs::write(
        source_dir.join("wikipedia_literature.toml"),
        r#"[source]
key = "books.wikipedia.literature"
name = "Literature Releases"
domain = "books"
enabled = true
timezone = "UTC"
jurisdiction = "GLOBAL"

[fetch]
mode = "file"
file_path = "../../data/literature.json"
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
event_type = "book_release"
status = "scheduled"
categories = ["culture", "books", "wikipedia", "literature"]
importance = 50

[custom]
enabled = true
parser = "wikipedia_release_table_v1"

[publish]
file_name_template = "literature-releases-{{year}}.ics"
"#,
    )?;

    fs::write(
        bundle_dir.join("culture_releases.toml"),
        r#"[bundle]
key = "culture.releases"
name = "Culture Releases"

[include]
source_patterns = [
  "television.us.wikipedia.*",
  "books.wikipedia.*",
]

[publish]
file_name_template = "culture-releases-{{year}}.ics"
"#,
    )?;

    fs::write(
        data_dir.join("television.json"),
        r#"{
  "parse": {
    "title": "2026 in American television",
    "text": "<table class=\"wikitable\"><tr><th>First aired</th><th>Title</th><th>Channel</th><th>Source</th></tr><tr><td>January 5</td><td><a href=\"/wiki/Example_Show_One\">Example Show One</a></td><td>Example TV</td><td>[1]</td></tr><tr><td></td><td><a href=\"/wiki/Example_Show_Two\">Example Show Two</a></td><td>Example TV</td><td>[2]</td></tr><tr><td>Spring</td><td><a href=\"/wiki/Example_Show_Three\">Example Show Three</a></td><td>Example Streamer</td><td>[3]</td></tr></table>"
  }
}"#,
    )?;

    fs::write(
        data_dir.join("literature.json"),
        r#"{
  "parse": {
    "title": "2026 in literature",
    "text": "<table class=\"wikitable\"><tr><th>Author</th><th>Title</th><th>Date of pub.</th><th>Ref.</th></tr><tr><td>Author One</td><td><a href=\"/wiki/Example_Book_One\">Example Book One</a></td><td>January 6</td><td>[1]</td></tr><tr><td>Author Two</td><td><a href=\"/wiki/Example_Book_Two\">Example Book Two</a></td><td>April 2 (UK)</td><td>[2]</td></tr></table>"
  }
}"#,
    )?;

    Ok(TempEnv {
        config_dir: root.join("sources"),
        state_path: root.join("state.json"),
        out_dir: root.join("out"),
    })
}

fn setup_more_temp_env() -> Result<TempEnv> {
    let temp = tempdir()?;
    let root = temp.keep();
    let source_dir = root.join("sources").join("culture");
    let data_dir = root.join("data");
    fs::create_dir_all(&source_dir)?;
    fs::create_dir_all(&data_dir)?;

    fs::write(
        source_dir.join("wikipedia_anime.toml"),
        r#"[source]
key = "anime.wikipedia.television"
name = "Anime Television Releases"
domain = "anime"
enabled = true
timezone = "UTC"
jurisdiction = "GLOBAL"

[fetch]
mode = "file"
file_path = "../../data/anime.json"
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
event_type = "anime_release"
status = "scheduled"
categories = ["culture", "anime", "wikipedia", "television"]
importance = 55

[custom]
enabled = true
parser = "wikipedia_release_table_v1"

[publish]
file_name_template = "anime-television-{{year}}.ics"
"#,
    )?;

    fs::write(
        source_dir.join("wikipedia_british_television.toml"),
        r#"[source]
key = "television.gb.wikipedia.debuts"
name = "British Television Debuts"
domain = "television"
enabled = true
timezone = "UTC"
jurisdiction = "GB"
default_country = "GB"

[fetch]
mode = "file"
file_path = "../../data/british_tv.json"
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
event_type = "television_release"
status = "scheduled"
categories = ["culture", "television", "wikipedia", "british_television"]
importance = 55

[custom]
enabled = true
parser = "wikipedia_release_table_v1"

[publish]
file_name_template = "british-television-{{year}}.ics"
"#,
    )?;

    fs::write(
        data_dir.join("anime.json"),
        r#"{
  "parse": {
    "title": "2026 in anime",
    "text": "<table class=\"wikitable\"><tr><th>First run start and end dates</th><th>Title</th><th>Episodes</th><th>Studio</th><th>Director(s)</th><th>Original title</th><th>Ref</th></tr><tr><td>January 3 – March 28</td><td><a href=\"/wiki/Example_Anime_One\">Example Anime One</a></td><td>13</td><td>Studio One</td><td>Director One</td><td>Original One</td><td>[1]</td></tr></table>"
  }
}"#,
    )?;

    fs::write(
        data_dir.join("british_tv.json"),
        r#"{
  "parse": {
    "title": "2026 in British television",
    "text": "<table class=\"wikitable\"><tr><th>Date</th><th>Debut</th><th>Channel</th></tr><tr><td>1 January</td><td><a href=\"/wiki/Example_UK_Show_One\">Example UK Show One</a></td><td>BBC One</td></tr><tr><td>3 January</td><td><a href=\"/wiki/Example_UK_Show_Two\">Example UK Show Two</a></td><td>ITV</td></tr></table>"
  }
}"#,
    )?;

    Ok(TempEnv {
        config_dir: root.join("sources"),
        state_path: root.join("state.json"),
        out_dir: root.join("out"),
    })
}
