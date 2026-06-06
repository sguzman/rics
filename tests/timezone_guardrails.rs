use anyhow::Result;
use rics::pipeline::{SyncOptions, sync_sources};
use std::fs;
use std::path::PathBuf;
use tempfile::tempdir;
use walkdir::WalkDir;

#[test]
fn structured_feed_rejects_naive_datetime_with_utc_timezone() -> Result<()> {
    let env = setup_env(
        "UTC",
        "2026-07-10 19:00 | Test Event | source_event_id=test-1 | source_url=https://example.com\n",
    )?;

    let result = sync_sources(&SyncOptions {
        config_dir: env.config_dir.clone(),
        state_path: env.state_path.clone(),
        out_dir: env.out_dir.clone(),
        source: None,
        dry_run: false,
    });

    assert!(result.is_err());
    let message = format!("{:#}", result.unwrap_err());
    assert!(message.contains("naive datetime"));
    Ok(())
}

#[test]
fn structured_feed_allows_naive_datetime_with_real_local_timezone() -> Result<()> {
    let env = setup_env(
        "America/New_York",
        "2026-07-10 19:00 | Test Event | source_event_id=test-1 | source_url=https://example.com\n",
    )?;

    let reports = sync_sources(&SyncOptions {
        config_dir: env.config_dir.clone(),
        state_path: env.state_path.clone(),
        out_dir: env.out_dir.clone(),
        source: None,
        dry_run: false,
    })?;

    assert_eq!(reports.len(), 1);
    let out_file = WalkDir::new(&env.out_dir)
        .into_iter()
        .filter_map(Result::ok)
        .map(|entry| entry.into_path())
        .find(|path| path.extension().and_then(|v| v.to_str()) == Some("ics"))
        .expect("expected generated ics file");
    let content = fs::read_to_string(out_file)?;
    assert!(content.contains("DTSTART:20260710T230000Z"));
    Ok(())
}

struct TempEnv {
    config_dir: PathBuf,
    state_path: PathBuf,
    out_dir: PathBuf,
}

fn setup_env(timezone: &str, body: &str) -> Result<TempEnv> {
    let temp = tempdir()?;
    let root = temp.keep();
    let config_dir = root.join("sources");
    let data_dir = root.join("data");
    let out_dir = root.join("out");
    fs::create_dir_all(&config_dir)?;
    fs::create_dir_all(&data_dir)?;
    fs::create_dir_all(&out_dir)?;

    fs::write(
        config_dir.join("source.toml"),
        format!(
            r#"[source]
key = "test.timezone.guard"
name = "Test Timezone Guard"
domain = "test"
enabled = true
timezone = "{timezone}"

[fetch]
mode = "file"
file_path = "../data/feed.txt"
timeout_secs = 10
retry_attempts = 1
retry_backoff_ms = 10

[extract]
format = "text"

[date]
primary = "date"
formats = ["%Y-%m-%d %H:%M", "%Y-%m-%d", "%Y"]
assume_timezone = "{timezone}"
allow_month_only = true
allow_year_only = true

[event]
event_type = "test_event"
status = "scheduled"
categories = ["test"]
importance = 50

[custom]
enabled = true
parser = "structured_calendar_feed_v1"

[publish]
file_name_template = "test-timezone-guard-{{year}}.ics"
"#
        ),
    )?;

    fs::write(data_dir.join("feed.txt"), body)?;

    Ok(TempEnv {
        config_dir,
        state_path: root.join("state.json"),
        out_dir,
    })
}
