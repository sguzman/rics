use anyhow::Result;
use rics::config::{load_bundles_from_dir, load_sources_from_dir};
use rics::pipeline::{sync_sources, SyncOptions};
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use tempfile::tempdir;

#[test]
fn competitive_card_game_sources_and_bundle_validate() -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let sources = load_sources_from_dir(&root.join("configs/sources/card_games"))?;
    let keys = sources
        .iter()
        .map(|source| source.config.source.key.as_str())
        .collect::<HashSet<_>>();

    assert_eq!(sources.len(), 7);
    assert!(keys.contains("card_games.magic_the_gathering"));
    assert!(keys.contains("card_games.pokemon_tcg"));
    assert!(keys.contains("card_games.yugioh_tcg"));
    assert!(keys.contains("card_games.hearthstone"));
    assert!(keys.contains("card_games.marvel_snap"));
    assert!(keys.contains("card_games.disney_lorcana"));
    assert!(keys.contains("card_games.flesh_and_blood"));

    let bundles = load_bundles_from_dir(&root.join("configs/bundles"))?;
    assert!(bundles
        .iter()
        .any(|bundle| bundle.config.bundle.key == "card_games.competitive"));
    Ok(())
}

#[test]
fn competitive_card_game_bundle_builds_from_real_sources() -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let temp = tempdir()?;
    let out_dir = temp.path().join("out");
    let state_path = temp.path().join("state.json");

    sync_sources(&SyncOptions {
        config_dir: root.join("configs/sources"),
        state_path,
        out_dir: out_dir.clone(),
        source: Some("card_games.magic_the_gathering".to_string()),
        dry_run: false,
    })?;
    sync_sources(&SyncOptions {
        config_dir: root.join("configs/sources"),
        state_path: temp.path().join("state.json"),
        out_dir: out_dir.clone(),
        source: Some("card_games.yugioh_tcg".to_string()),
        dry_run: false,
    })?;

    let bundle_2026 = out_dir
        .join("bundles")
        .join("card-games-competitive")
        .join("card-games-2026.ics");
    assert!(bundle_2026.exists());

    let content = fs::read_to_string(bundle_2026)?;
    assert!(content.contains(
        "SUMMARY:Magic: The Gathering Premier Play: Pro Tour Lorwyn Eclipsed"
    ));
    assert!(content.contains(
        "SUMMARY:Yu-Gi-Oh! TCG Organized Play: YCS Columbus"
    ));
    Ok(())
}
