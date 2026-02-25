use anyhow::Result;
use clap::{Parser, Subcommand};
use rics::harness::{HarnessOptions, run_harness};
use rics::pipeline::{
    BuildOptions, SyncOptions, ValidateOptions, build_calendars, sync_sources, validate_configs,
};
use std::path::PathBuf;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "rics", about = "Config-driven calendar ICS generator")]
struct Cli {
    #[arg(long, default_value = "configs/sources")]
    config_dir: PathBuf,

    #[arg(long, default_value = "data/state/events.json")]
    state_path: PathBuf,

    #[arg(long, default_value = "data/out")]
    out_dir: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Sync {
        #[arg(long)]
        source: Option<String>,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
    Build {
        #[arg(long)]
        source: Option<String>,
        #[arg(long)]
        year: Option<i32>,
    },
    Validate {
        #[arg(long)]
        source_file: Option<PathBuf>,
    },
    Harness,
}

fn main() -> Result<()> {
    init_tracing()?;
    let cli = Cli::parse();

    match cli.command {
        Commands::Sync { source, dry_run } => {
            let reports = sync_sources(&SyncOptions {
                config_dir: cli.config_dir,
                state_path: cli.state_path,
                out_dir: cli.out_dir,
                source,
                dry_run,
            })?;

            for report in reports {
                info!(
                    source = %report.source_key,
                    pages = report.pages_fetched,
                    parsed = report.records_parsed,
                    inserted = report.inserted,
                    updated = report.updated,
                    unchanged = report.unchanged,
                    cancelled = report.cancelled,
                    "source sync summary"
                );
            }
        }
        Commands::Build { source, year } => {
            build_calendars(&BuildOptions {
                config_dir: cli.config_dir,
                state_path: cli.state_path,
                out_dir: cli.out_dir,
                source,
                year,
            })?;
            info!("build complete");
        }
        Commands::Validate { source_file } => {
            let messages = validate_configs(&ValidateOptions {
                config_dir: Some(cli.config_dir),
                source_file,
            })?;
            for line in messages {
                println!("{line}");
            }
        }
        Commands::Harness => {
            let report = run_harness(&HarnessOptions {
                config_dir: cli.config_dir,
                state_path: cli.state_path,
                out_dir: cli.out_dir,
            })?;

            println!("{}", serde_json::to_string_pretty(&report)?);
        }
    }

    Ok(())
}

fn init_tracing() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .try_init()
        .map_err(|err| anyhow::anyhow!(err.to_string()))?;
    Ok(())
}
