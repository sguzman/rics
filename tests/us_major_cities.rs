use anyhow::Result;
use rics::config::{load_bundles_from_dir, load_sources_from_dir};
use rics::pipeline::{SyncOptions, sync_sources};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::tempdir;

#[test]
fn us_major_city_sources_validate_and_have_expected_keys() -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let sources = load_sources_from_dir(&root.join("configs/sources/cities/us_major"))?;
    let bundles = load_bundles_from_dir(&root.join("configs/bundles"))?;

    assert_eq!(sources.len(), 6);
    assert!(bundles
        .iter()
        .any(|bundle| bundle.config.bundle.key == "us_major_cities.civic"));

    let keys = sources
        .into_iter()
        .map(|source| source.config.source.key)
        .collect::<HashSet<_>>();
    assert!(keys.contains("cities.us.nyc"));
    assert!(keys.contains("cities.us.los_angeles"));
    assert!(keys.contains("cities.us.chicago"));
    assert!(keys.contains("cities.us.houston"));
    assert!(keys.contains("cities.us.philadelphia"));
    assert!(keys.contains("cities.us.phoenix"));

    Ok(())
}

#[test]
fn us_major_city_bundle_builds_from_multiple_cities() -> Result<()> {
    let env = setup_temp_city_env()?;

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
        .join("us-major-cities-civic")
        .join("us-major-cities-civic-2026.ics");
    assert!(bundle_2026.exists());

    let content = fs::read_to_string(bundle_2026)?;
    assert!(content.contains("SUMMARY:New York City: Council calendar published"));
    assert!(content.contains("SUMMARY:Los Angeles: Planning Commission hearing"));

    Ok(())
}

struct TempCityEnv {
    config_dir: PathBuf,
    state_path: PathBuf,
    out_dir: PathBuf,
}

fn setup_temp_city_env() -> Result<TempCityEnv> {
    let temp = tempdir()?;
    let root = temp.keep();

    let config_dir = root.join("sources");
    let bundle_dir = root.join("bundles");
    let data_dir = root.join("data");
    fs::create_dir_all(&config_dir)?;
    fs::create_dir_all(&bundle_dir)?;
    fs::create_dir_all(&data_dir)?;

    fs::write(
        config_dir.join("nyc.toml"),
        r#"[source]
key = "cities.us.nyc"
name = "New York City"
domain = "cities"
enabled = true
timezone = "UTC"
jurisdiction = "US-NY-NYC"
default_country = "US"

[fetch]
mode = "file"
file_path = "../data/nyc.txt"
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
event_type = "city_event"
status = "scheduled"
categories = ["cities", "us_major", "civic", "nyc"]
importance = 80

[custom]
enabled = true
parser = "structured_calendar_feed_v1"

[publish]
file_name_template = "us-city-nyc-{{year}}.ics"
"#,
    )?;

    fs::write(
        config_dir.join("la.toml"),
        r#"[source]
key = "cities.us.la"
name = "Los Angeles"
domain = "cities"
enabled = true
timezone = "UTC"
jurisdiction = "US-CA-LA"
default_country = "US"

[fetch]
mode = "file"
file_path = "../data/la.txt"
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
event_type = "city_event"
status = "scheduled"
categories = ["cities", "us_major", "civic", "los_angeles"]
importance = 80

[custom]
enabled = true
parser = "structured_calendar_feed_v1"

[publish]
file_name_template = "us-city-la-{{year}}.ics"
"#,
    )?;

    fs::write(
        bundle_dir.join("us_major_cities.toml"),
        r#"[bundle]
key = "us_major_cities.civic"
name = "US Major Cities Civic"

[include]
source_patterns = ["cities.us.*"]

[publish]
file_name_template = "us-major-cities-civic-{{year}}.ics"
"#,
    )?;

    fs::write(
        data_dir.join("nyc.txt"),
        concat!(
            "2026 | Council calendar published | subtype=city_council_calendar | office=city_council | source_class=official | source_event_id=test-nyc-calendar | source_url=https://legistar.council.nyc.gov/ | description=Calendar.\\n",
        ),
    )?;

    fs::write(
        data_dir.join("la.txt"),
        concat!(
            "2026-05-14 | Planning Commission hearing | subtype=planning_hearing | office=city_planning_commission | source_class=official | source_event_id=test-la-hearing | source_url=https://planning.lacity.gov/about/calendar | description=Hearing.\\n",
        ),
    )?;

    Ok(TempCityEnv {
        config_dir,
        state_path: root.join("state.json"),
        out_dir: root.join("out"),
    })
}
