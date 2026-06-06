use anyhow::Result;
use rics::config::{load_bundles_from_dir, load_sources_from_dir};
use rics::pipeline::{SyncOptions, sync_sources};
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use tempfile::tempdir;

#[test]
fn us_treasury_markets_source_pack_validates_and_has_expected_keys() -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let sources = load_sources_from_dir(&root.join("configs/sources/finance"))?;
    let bundles = load_bundles_from_dir(&root.join("configs/bundles"))?;

    let keys = sources
        .iter()
        .map(|source| source.config.source.key.as_str())
        .collect::<HashSet<_>>();

    assert!(keys.contains("finance.us_treasury_markets.refunding"));
    assert!(
        bundles
            .iter()
            .any(|bundle| bundle.config.bundle.key == "finance.us_treasury_markets")
    );

    Ok(())
}

#[test]
fn us_treasury_markets_bundle_builds_from_real_source() -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let temp = tempdir()?;
    let out_dir = temp.path().join("out");
    let state_path = temp.path().join("state.json");

    sync_sources(&SyncOptions {
        config_dir: root.join("configs/sources"),
        state_path,
        out_dir: out_dir.clone(),
        source: Some("finance.us_treasury_markets.refunding".to_string()),
        dry_run: false,
    })?;

    let bundle_2026 = out_dir
        .join("bundles")
        .join("finance-us-treasury-markets")
        .join("us-treasury-markets-2026.ics");
    assert!(bundle_2026.exists());

    let content = fs::read_to_string(bundle_2026)?;
    assert!(
        content.contains(
            "SUMMARY:US Treasury Markets: Quarterly Refunding: Treasury quarterly refund"
        )
    );
    assert!(
        content.contains(
            "SUMMARY:US Treasury Markets: Quarterly Refunding: Treasury buyback schedule"
        )
    );
    assert!(content.contains("financing estimates scheduled for Q3 2026"));

    Ok(())
}
