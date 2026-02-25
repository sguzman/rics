use crate::model::State;
use anyhow::{Context, Result};
use std::path::Path;

pub fn load_state(path: &Path) -> Result<State> {
    if !path.exists() {
        return Ok(State::default());
    }

    let content = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read state file {}", path.display()))?;
    let state = serde_json::from_str(&content)
        .with_context(|| format!("failed to parse state file {}", path.display()))?;
    Ok(state)
}

pub fn save_state(path: &Path, state: &State) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create state directory {}", parent.display()))?;
    }

    let serialized = serde_json::to_string_pretty(state)?;
    std::fs::write(path, serialized)
        .with_context(|| format!("failed to write state file {}", path.display()))?;
    Ok(())
}
