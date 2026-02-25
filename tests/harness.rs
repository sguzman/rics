use anyhow::Result;
use rics::harness::{HarnessOptions, run_harness};
use rics::pipeline::{SyncOptions, load_state_for_read, sync_sources};
use std::fs;
use std::path::Path;
use tempfile::tempdir;

#[test]
fn sync_builds_yearly_ics_files() -> Result<()> {
    let env = setup_fixture_env()?;

    let reports = sync_sources(&SyncOptions {
        config_dir: env.config_dir.clone(),
        state_path: env.state_path.clone(),
        out_dir: env.out_dir.clone(),
        source: None,
        dry_run: false,
    })?;

    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].inserted, 2);

    let y2026 = env
        .out_dir
        .join("sources")
        .join("test-oecd-fixture")
        .join("test-oecd-fixture-2026.ics");
    let y2027 = env
        .out_dir
        .join("sources")
        .join("test-oecd-fixture")
        .join("test-oecd-fixture-2027.ics");

    assert!(y2026.exists());
    assert!(y2027.exists());

    let content = fs::read_to_string(y2026)?;
    assert!(content.contains("SUMMARY:OECD Sample Report A"));
    assert!(content.contains("X-RICS-SOURCE-KEY:test.oecd.fixture"));

    Ok(())
}

#[test]
fn sync_updates_existing_future_events() -> Result<()> {
    let env = setup_fixture_env()?;

    sync_sources(&SyncOptions {
        config_dir: env.config_dir.clone(),
        state_path: env.state_path.clone(),
        out_dir: env.out_dir.clone(),
        source: None,
        dry_run: false,
    })?;

    let fixture_html = env.data_dir.join("oecd_fixture.html");
    let html = fs::read_to_string(&fixture_html)?;
    let updated = html
        .replace("OECD Sample Report A", "OECD Sample Report A Revised")
        .replace("2026-05-01", "2026-05-20");
    fs::write(&fixture_html, updated)?;

    let reports = sync_sources(&SyncOptions {
        config_dir: env.config_dir.clone(),
        state_path: env.state_path.clone(),
        out_dir: env.out_dir.clone(),
        source: None,
        dry_run: false,
    })?;

    assert_eq!(reports[0].updated, 1);

    let state = load_state_for_read(&env.state_path)?;
    let record = state
        .events
        .values()
        .find(|event| {
            event
                .source_url
                .as_deref()
                .is_some_and(|url| url.contains("sample-report-a_123"))
        })
        .expect("record for sample-report-a must exist");

    assert_eq!(record.sequence, 1);
    assert!(record.title.contains("Revised"));

    let content = fs::read_to_string(
        env.out_dir
            .join("sources")
            .join("test-oecd-fixture")
            .join("test-oecd-fixture-2026.ics"),
    )?;
    assert!(content.contains("SEQUENCE:1"));

    Ok(())
}

#[test]
fn harness_reports_stability_metrics() -> Result<()> {
    let env = setup_fixture_env()?;

    let report = run_harness(&HarnessOptions {
        config_dir: env.config_dir,
        state_path: env.state_path,
        out_dir: env.out_dir,
    })?;

    assert_eq!(report.first_run_inserted, 2);
    assert_eq!(report.second_run_inserted, 0);
    assert_eq!(report.second_run_updated, 0);
    assert!(report.ics_files >= 2);

    Ok(())
}

struct FixtureEnv {
    config_dir: std::path::PathBuf,
    data_dir: std::path::PathBuf,
    state_path: std::path::PathBuf,
    out_dir: std::path::PathBuf,
}

fn setup_fixture_env() -> Result<FixtureEnv> {
    let temp = tempdir()?;
    let root = temp.keep();

    let fixture_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let config_dir = root.join("sources");
    let data_dir = root.join("data");
    copy_dir(&fixture_root.join("sources"), &config_dir)?;
    copy_dir(&fixture_root.join("data"), &data_dir)?;

    let state_path = root.join("state/events.json");
    let out_dir = root.join("out");

    Ok(FixtureEnv {
        config_dir,
        data_dir,
        state_path,
        out_dir,
    })
}

fn copy_dir(src: &Path, dst: &Path) -> Result<()> {
    fs::create_dir_all(dst)?;

    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir(&src_path, &dst_path)?;
        } else {
            if let Some(parent) = dst_path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(src_path, dst_path)?;
        }
    }

    Ok(())
}
