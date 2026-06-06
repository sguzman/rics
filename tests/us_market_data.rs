use anyhow::Result;
use rics::config::{load_bundles_from_dir, load_sources_from_dir};
use rics::pipeline::{SyncOptions, sync_sources};
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use tempfile::tempdir;

#[test]
fn us_sec_edgar_and_market_data_packs_validate() -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let sources = load_sources_from_dir(&root.join("configs/sources/finance"))?;
    let bundles = load_bundles_from_dir(&root.join("configs/bundles"))?;

    let keys = sources
        .iter()
        .map(|source| source.config.source.key.as_str())
        .collect::<HashSet<_>>();

    assert!(keys.contains("finance.us_sec_edgar.filing_deadlines"));
    assert!(keys.contains("finance.us_market_data.fed_stats"));
    assert!(keys.contains("finance.us_market_data.eia"));
    assert!(
        bundles
            .iter()
            .any(|bundle| bundle.config.bundle.key == "finance.us_sec_edgar")
    );
    assert!(
        bundles
            .iter()
            .any(|bundle| bundle.config.bundle.key == "finance.us_market_data")
    );

    Ok(())
}

#[test]
fn us_sec_edgar_and_market_data_bundles_build_from_real_sources() -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let temp = tempdir()?;
    let out_dir = temp.path().join("out");
    let state_path = temp.path().join("state.json");

    for key in [
        "finance.us_sec_edgar.filing_deadlines",
        "finance.us_market_data.fed_stats",
        "finance.us_market_data.eia",
    ] {
        sync_sources(&SyncOptions {
            config_dir: root.join("configs/sources"),
            state_path: state_path.clone(),
            out_dir: out_dir.clone(),
            source: Some(key.to_string()),
            dry_run: false,
        })?;
    }

    let sec_bundle = out_dir
        .join("bundles")
        .join("finance-us-sec-edgar")
        .join("us-sec-edgar-2026.ics");
    let market_bundle = out_dir
        .join("bundles")
        .join("finance-us-market-data")
        .join("us-market-data-2026.ics");

    assert!(sec_bundle.exists());
    assert!(market_bundle.exists());

    let sec_content = fs::read_to_string(sec_bundle)?;
    assert!(
        sec_content.contains("SUMMARY:US SEC/EDGAR Filing Deadlines: Form 13F filing deadline")
    );
    assert!(
        sec_content.contains("SUMMARY:US SEC/EDGAR Filing Deadlines: Form 10-K filing deadline")
    );

    let market_content = fs::read_to_string(market_bundle)?;
    assert!(market_content.contains("H.6 Money Stock Measures release"));
    assert!(market_content.contains("Short-Term Energy Outlook release"));
    assert!(market_content.contains("Weekly Petroleum Status Report holiday release"));

    Ok(())
}
