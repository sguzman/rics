use anyhow::Result;
use rics::pipeline::{SyncOptions, sync_sources};
use std::fs;
use std::path::Path;
use tempfile::tempdir;

#[test]
fn deepened_central_bank_sources_build_exact_2026_calendars() -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let temp = tempdir()?;
    let out_dir = temp.path().join("out");
    let state_path = temp.path().join("state.json");

    for key in [
        "macro.central_banks.ecb",
        "macro.central_banks.boe",
        "macro.central_banks.boc",
        "macro.central_banks.rba",
        "macro.central_banks.rbnz",
    ] {
        sync_sources(&SyncOptions {
            config_dir: root.join("configs/sources"),
            state_path: state_path.clone(),
            out_dir: out_dir.clone(),
            source: Some(key.to_string()),
            dry_run: false,
        })?;
    }

    let bundle = out_dir
        .join("bundles")
        .join("macro-central-banks")
        .join("central-banks-2026.ics");
    assert!(bundle.exists());
    let content = fs::read_to_string(bundle)?;

    assert!(content.contains("Central Bank: European Central Bank: ECB Governing Council monetary"));
    assert!(content.contains("Central Bank: Bank of England: Bank of England MPC decision"));
    assert!(content.contains("Central Bank: Bank of Canada: Bank of Canada interest rate"));
    assert!(content.contains("Central Bank: Reserve Bank of Australia: RBA Monetary Policy"));
    assert!(content.contains("Central Bank: Reserve Bank of New Zealand: RBNZ Monetary Policy"));
    assert!(content.contains("DTSTART;VALUE=DATE:20260204"));
    assert!(content.contains("DTSTART;VALUE=DATE:20260205"));
    assert!(content.contains("DTSTART;VALUE=DATE:20260128"));
    assert!(content.contains("DTSTART;VALUE=DATE:20260202"));
    assert!(content.contains("DTSTART;VALUE=DATE:20260408"));

    Ok(())
}
