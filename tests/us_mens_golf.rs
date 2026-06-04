use anyhow::Result;
use rics::config::{load_bundles_from_dir, load_sources_from_dir};
use rics::pipeline::{sync_sources, SyncOptions};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::tempdir;

#[test]
fn us_mens_golf_sources_validate_and_have_expected_keys() -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let sources = load_sources_from_dir(&root.join("configs/sources/sports/us_mens_golf"))?;
    let bundles = load_bundles_from_dir(&root.join("configs/bundles"))?;

    assert_eq!(sources.len(), 4);
    assert!(bundles
        .iter()
        .any(|bundle| bundle.config.bundle.key == "us_mens_golf.all"));

    let keys = sources
        .into_iter()
        .map(|source| source.config.source.key)
        .collect::<HashSet<_>>();
    assert!(keys.contains("sports.golf.pga_tour"));
    assert!(keys.contains("sports.golf.pga_tour_champions"));
    assert!(keys.contains("sports.golf.korn_ferry"));
    assert!(keys.contains("sports.golf.liv"));

    Ok(())
}

#[test]
fn us_mens_golf_bundle_builds_from_multiple_tours() -> Result<()> {
    let env = setup_temp_golf_env()?;

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
        .join("us-mens-golf-all")
        .join("us-mens-golf-2026.ics");
    assert!(bundle_2026.exists());

    let content = fs::read_to_string(bundle_2026)?;
    assert!(content.contains("SUMMARY:PGA TOUR: Test Classic"));
    assert!(content.contains("SUMMARY:LIV Golf Test City"));

    Ok(())
}

struct TempGolfEnv {
    config_dir: PathBuf,
    state_path: PathBuf,
    out_dir: PathBuf,
}

fn setup_temp_golf_env() -> Result<TempGolfEnv> {
    let temp = tempdir()?;
    let root = temp.keep();

    let config_dir = root.join("sources");
    let bundle_dir = root.join("bundles");
    let data_dir = root.join("data");
    fs::create_dir_all(&config_dir)?;
    fs::create_dir_all(&bundle_dir)?;
    fs::create_dir_all(&data_dir)?;

    fs::write(
        config_dir.join("pga.toml"),
        r#"[source]
key = "sports.golf.pga_tour"
name = "PGA TOUR"
domain = "sports"
enabled = true
timezone = "UTC"
jurisdiction = "GLOBAL"

[fetch]
mode = "file"
file_path = "../data/pga.html"
timeout_secs = 10
retry_attempts = 1
retry_backoff_ms = 10

[extract]
format = "html"

[date]
primary = "date"
formats = ["%Y-%m-%d", "%Y"]
assume_timezone = "UTC"
allow_month_only = true
allow_year_only = true

[event]
event_type = "sports_event"
status = "scheduled"
categories = ["sports", "golf", "mens", "pga_tour"]
importance = 80

[custom]
enabled = true
parser = "pgatour_schedule_next_data_v1"

[publish]
file_name_template = "us-mens-golf-pga-tour-{{year}}.ics"
"#,
    )?;

    fs::write(
        config_dir.join("liv.toml"),
        r#"[source]
key = "sports.golf.liv"
name = "LIV Golf"
domain = "sports"
enabled = true
timezone = "UTC"
jurisdiction = "GLOBAL"

[fetch]
mode = "file"
file_path = "../data/liv.txt"
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
categories = ["sports", "golf", "mens", "liv"]
importance = 80

[custom]
enabled = true
parser = "structured_calendar_feed_v1"

[publish]
file_name_template = "us-mens-golf-liv-{{year}}.ics"
"#,
    )?;

    fs::write(
        bundle_dir.join("us_mens_golf.toml"),
        r#"[bundle]
key = "us_mens_golf.all"
name = "US Men's Golf"

[include]
source_patterns = ["sports.golf.*"]

[publish]
file_name_template = "us-mens-golf-{{year}}.ics"
"#,
    )?;

    fs::write(
        data_dir.join("pga.html"),
        concat!(
            "<html><body><script id=\"__NEXT_DATA__\" type=\"application/json\">",
            "{\"props\":{\"pageProps\":{\"dehydratedState\":{\"queries\":[{\"state\":{\"data\":{}}},{\"state\":{\"data\":{}}},{\"state\":{\"data\":{}}},{\"state\":{\"data\":{\"tournaments\":[",
            "{\"tournamentId\":\"R2026999\",\"name\":\"Test Classic\",\"year\":\"2026\",\"month\":\"March\",\"displayDate\":\"Mar 5 - 8\",\"dateAccessibilityText\":\"March 5th through March 8th\",\"display\":\"SHOW\",\"status\":\"UPCOMING\",\"courseData\":{\"name\":\"Test National\",\"city\":\"Austin\",\"stateCode\":\"TX\",\"country\":\"United States of America\",\"countryCode\":\"USA\"},\"ticketing\":{\"enabled\":true,\"ticketsUrl\":\"https://example.com/test-classic\"},\"purse\":\"$9,000,000\",\"standings\":{\"heading\":\"FEDEXCUP\",\"value\":\"500 pts\"},\"champions\":[{\"displayName\":\"Sample Winner\"}]},",
            "{\"tournamentId\":\"R2026000\",\"name\":\"Cross-Month Open\",\"year\":\"2026\",\"month\":\"February\",\"displayDate\":\"Feb 26 - Mar 1\",\"dateAccessibilityText\":\"February 26th through March 1st\",\"display\":\"SHOW\",\"status\":\"UPCOMING\",\"courseData\":{\"name\":\"Cross-Month Course\",\"city\":\"Palm Beach Gardens\",\"stateCode\":\"FL\",\"country\":\"United States of America\",\"countryCode\":\"USA\"},\"ticketing\":{\"enabled\":false,\"ticketsUrl\":null},\"purse\":\"$9,600,000\",\"standings\":{\"heading\":\"FEDEXCUP\",\"value\":\"500 pts\"},\"champions\":[]}",
            "]}}}]}}}}</script></body></html>",
        ),
    )?;

    fs::write(
        data_dir.join("liv.txt"),
        "2026-06-25 | LIV Golf Test City | end=2026-06-28 | subtype=golf_tournament | tour=liv | course=Test Course | source_class=official | source_event_id=liv-test-city-2026 | source_url=https://example.com/liv-test-city | description=Test LIV event.\\n",
    )?;

    Ok(TempGolfEnv {
        config_dir,
        state_path: root.join("state.json"),
        out_dir: root.join("out"),
    })
}
