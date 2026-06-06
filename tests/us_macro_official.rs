use anyhow::Result;
use rics::config::{load_bundles_from_dir, load_sources_from_dir};
use rics::pipeline::{sync_sources, SyncOptions};
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use tempfile::tempdir;

#[test]
fn us_macro_official_source_pack_validates_and_has_expected_keys() -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let sources = load_sources_from_dir(&root.join("configs/sources/finance"))?;
    let bundles = load_bundles_from_dir(&root.join("configs/bundles"))?;

    let keys = sources
        .iter()
        .map(|source| source.config.source.key.as_str())
        .collect::<HashSet<_>>();

    assert_eq!(sources.len(), 4);
    assert!(keys.contains("finance.us_macro_official.bls"));
    assert!(keys.contains("finance.us_macro_official.bea"));
    assert!(keys.contains("finance.us_macro_official.census"));
    assert!(keys.contains("finance.us_macro_official.fed"));
    assert!(bundles
        .iter()
        .any(|bundle| bundle.config.bundle.key == "finance.us_macro_official"));

    for source in sources {
        assert_eq!(source.config.source.default_country.as_deref(), Some("US"));
    }

    Ok(())
}

#[test]
fn us_macro_official_bundle_builds_from_real_sources() -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let temp = tempdir()?;
    let out_dir = temp.path().join("out");
    let state_path = temp.path().join("state.json");

    for key in [
        "finance.us_macro_official.bls",
        "finance.us_macro_official.bea",
        "finance.us_macro_official.census",
        "finance.us_macro_official.fed",
    ] {
        sync_sources(&SyncOptions {
            config_dir: root.join("configs/sources"),
            state_path: state_path.clone(),
            out_dir: out_dir.clone(),
            source: Some(key.to_string()),
            dry_run: false,
        })?;
    }

    let bundle_2026 = out_dir
        .join("bundles")
        .join("finance-us-macro-official")
        .join("us-macro-official-2026.ics");
    assert!(bundle_2026.exists());

    let content = fs::read_to_string(bundle_2026)?;
    assert!(content.contains("Consumer Price Index for May 2026"));
    assert!(content.contains("GDP Advance Estimate for Q2 2026"));
    assert!(content.contains("SUMMARY:US Macro Official: Census: Advance Monthly Retail Trade Report"));
    assert!(content.contains("FOMC meeting begins"));

    Ok(())
}
