mod catalog;
mod config;
mod iceberg;
mod ingest;
mod input;
mod output;
mod pg_writer;
mod reader;
mod schema;
mod state;
mod sync;
mod types;
mod writer;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "rustream", about = "Fast Postgres → Parquet sync tool")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Sync tables from Postgres to Parquet files
    Sync {
        /// Path to config YAML file
        #[arg(short, long)]
        config: String,

        /// Show what would be synced without actually doing it
        #[arg(long)]
        dry_run: bool,
    },

    /// Ingest Parquet/CSV files from local filesystem or S3 into Postgres
    Ingest {
        /// Path to config YAML file
        #[arg(short, long)]
        config: String,

        /// Show what would be ingested without actually doing it
        #[arg(long)]
        dry_run: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("rustream=info".parse()?))
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Sync {
            config: config_path,
            dry_run,
        } => {
            let cfg = config::load(&config_path)?;
            if dry_run {
                sync::dry_run(cfg).await?;
            } else {
                sync::run(cfg).await?;
            }
        }
        Commands::Ingest {
            config: config_path,
            dry_run,
        } => {
            let cfg = config::load(&config_path)?;
            if dry_run {
                ingest::dry_run(cfg).await?;
            } else {
                ingest::run(cfg).await?;
            }
        }
    }

    Ok(())
}
