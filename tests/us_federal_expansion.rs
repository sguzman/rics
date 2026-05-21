use anyhow::Result;
use rics::config::{load_bundles_from_dir, load_sources_from_dir};
use std::collections::HashSet;
use std::path::Path;

#[test]
fn us_federal_additional_source_families_validate_and_have_expected_counts() -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let committee_sources = load_sources_from_dir(&root.join("configs/sources/federal/us_committees"))?;
    let court_sources = load_sources_from_dir(&root.join("configs/sources/federal/us_courts"))?;
    let agency_sources = load_sources_from_dir(&root.join("configs/sources/federal/us_agencies"))?;
    let bundles = load_bundles_from_dir(&root.join("configs/bundles"))?;

    assert_eq!(committee_sources.len(), 2);
    assert_eq!(court_sources.len(), 3);
    assert_eq!(agency_sources.len(), 4);

    let bundle_keys = bundles
        .into_iter()
        .map(|bundle| bundle.config.bundle.key)
        .collect::<HashSet<_>>();
    assert!(bundle_keys.contains("us_federal.committees"));
    assert!(bundle_keys.contains("us_federal.courts"));
    assert!(bundle_keys.contains("us_federal.agencies"));
    assert!(bundle_keys.contains("us_federal.all"));

    Ok(())
}
