use crate::pipeline::{SyncOptions, load_state_for_read, sync_sources};
use anyhow::Result;
use serde::Serialize;
use std::path::PathBuf;
use walkdir::WalkDir;

#[derive(Debug, Clone)]
pub struct HarnessOptions {
    pub config_dir: PathBuf,
    pub state_path: PathBuf,
    pub out_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize)]
pub struct HarnessReport {
    pub first_run_sources: usize,
    pub first_run_inserted: usize,
    pub first_run_updated: usize,
    pub first_run_cancelled: usize,
    pub second_run_inserted: usize,
    pub second_run_updated: usize,
    pub second_run_cancelled: usize,
    pub total_events: usize,
    pub ics_files: usize,
}

pub fn run_harness(options: &HarnessOptions) -> Result<HarnessReport> {
    if options.out_dir.exists() {
        std::fs::remove_dir_all(&options.out_dir)?;
    }
    if options.state_path.exists() {
        std::fs::remove_file(&options.state_path)?;
    }

    let first = sync_sources(&SyncOptions {
        config_dir: options.config_dir.clone(),
        state_path: options.state_path.clone(),
        out_dir: options.out_dir.clone(),
        source: None,
        dry_run: false,
    })?;

    let second = sync_sources(&SyncOptions {
        config_dir: options.config_dir.clone(),
        state_path: options.state_path.clone(),
        out_dir: options.out_dir.clone(),
        source: None,
        dry_run: false,
    })?;

    let state = load_state_for_read(&options.state_path)?;

    let mut ics_files = 0usize;
    for entry in WalkDir::new(&options.out_dir) {
        let entry = entry?;
        if entry.file_type().is_file()
            && entry.path().extension().and_then(|s| s.to_str()) == Some("ics")
        {
            ics_files += 1;
        }
    }

    Ok(HarnessReport {
        first_run_sources: first.len(),
        first_run_inserted: first.iter().map(|r| r.inserted).sum(),
        first_run_updated: first.iter().map(|r| r.updated).sum(),
        first_run_cancelled: first.iter().map(|r| r.cancelled).sum(),
        second_run_inserted: second.iter().map(|r| r.inserted).sum(),
        second_run_updated: second.iter().map(|r| r.updated).sum(),
        second_run_cancelled: second.iter().map(|r| r.cancelled).sum(),
        total_events: state.events.len(),
        ics_files,
    })
}
