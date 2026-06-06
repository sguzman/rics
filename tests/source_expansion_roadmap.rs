use anyhow::Result;
use rics::config::{load_bundles_from_dir, load_sources_from_dir};
use rics::pipeline::{SyncOptions, sync_sources};
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use tempfile::tempdir;

#[test]
fn source_expansion_packs_validate() -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let sources = load_sources_from_dir(&root.join("configs/sources"))?;
    let bundles = load_bundles_from_dir(&root.join("configs/bundles"))?;
    let keys = sources
        .iter()
        .map(|source| source.config.source.key.as_str())
        .collect::<HashSet<_>>();

    for key in [
        "regulation.us_federal_register.finance",
        "space.launches.nasa",
        "health.us.fda",
        "weather.us.noaa.cpc",
        "finance.us_company_ir.mega_cap_tech",
        "grants.us.nih",
        "transit.us.nyc",
        "macro.central_banks.ecb",
        "law.us.scotus",
    ] {
        assert!(keys.contains(key));
    }

    for bundle_key in [
        "regulation.us_federal_register",
        "space.launches.all",
        "health.us_public_health",
        "weather.us.noaa",
        "finance.us_company_ir",
        "grants.us_public_funding",
        "transit.us_major",
        "macro.central_banks",
        "law.us_judicial",
    ] {
        assert!(
            bundles
                .iter()
                .any(|bundle| bundle.config.bundle.key == bundle_key)
        );
    }

    Ok(())
}

#[test]
fn source_expansion_bundles_build_from_real_sources() -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let temp = tempdir()?;
    let out_dir = temp.path().join("out");
    let state_path = temp.path().join("state.json");

    for key in [
        "regulation.us_federal_register.finance",
        "regulation.us_federal_register.health",
        "regulation.us_federal_register.environment",
        "regulation.us_federal_register.transport",
        "regulation.us_federal_register.telecom",
        "space.launches.nasa",
        "space.launches.spacex",
        "space.launches.ula",
        "space.launches.rocket_lab",
        "space.launches.esa",
        "health.us.fda",
        "health.us.cdc",
        "weather.us.noaa.cpc",
        "weather.us.noaa.hurricane",
        "finance.us_company_ir.mega_cap_tech",
        "finance.us_company_ir.major_banks",
        "finance.us_company_ir.energy_majors",
        "grants.us.grants_gov",
        "grants.us.nih",
        "grants.us.nsf",
        "grants.us.sam",
        "transit.us.nyc",
        "transit.us.dc",
        "transit.us.chicago",
        "transit.us.los_angeles",
        "transit.us.sf_bay",
        "transit.us.boston",
        "macro.central_banks.ecb",
        "macro.central_banks.boe",
        "macro.central_banks.boj",
        "macro.central_banks.snb",
        "macro.central_banks.boc",
        "macro.central_banks.rba",
        "macro.central_banks.rbnz",
        "law.us.scotus",
        "law.us.appellate_major",
    ] {
        sync_sources(&SyncOptions {
            config_dir: root.join("configs/sources"),
            state_path: state_path.clone(),
            out_dir: out_dir.clone(),
            source: Some(key.to_string()),
            dry_run: false,
        })?;
    }

    for bundle_path in [
        out_dir
            .join("bundles")
            .join("regulation-us-federal-register")
            .join("us-federal-register-2026.ics"),
        out_dir
            .join("bundles")
            .join("space-launches-all")
            .join("space-launches-2026.ics"),
        out_dir
            .join("bundles")
            .join("health-us-public-health")
            .join("us-public-health-2026.ics"),
        out_dir
            .join("bundles")
            .join("weather-us-noaa")
            .join("us-noaa-2026.ics"),
        out_dir
            .join("bundles")
            .join("finance-us-company-ir")
            .join("us-company-ir-2026.ics"),
        out_dir
            .join("bundles")
            .join("grants-us-public-funding")
            .join("us-public-funding-2026.ics"),
        out_dir
            .join("bundles")
            .join("transit-us-major")
            .join("us-transit-major-2026.ics"),
        out_dir
            .join("bundles")
            .join("macro-central-banks")
            .join("central-banks-2026.ics"),
        out_dir
            .join("bundles")
            .join("law-us-judicial")
            .join("us-judicial-2026.ics"),
    ] {
        assert!(bundle_path.exists(), "missing {}", bundle_path.display());
    }

    let regulation = fs::read_to_string(
        out_dir
            .join("bundles")
            .join("regulation-us-federal-register")
            .join("us-federal-register-2026.ics"),
    )?;
    let space = fs::read_to_string(
        out_dir
            .join("bundles")
            .join("space-launches-all")
            .join("space-launches-2026.ics"),
    )?;
    let macro_ics = fs::read_to_string(
        out_dir
            .join("bundles")
            .join("macro-central-banks")
            .join("central-banks-2026.ics"),
    )?;

    assert!(regulation.contains("Federal Register"));
    assert!(space.contains("launch and mission calendar"));
    assert!(macro_ics.contains("policy calendar 2026"));

    Ok(())
}
