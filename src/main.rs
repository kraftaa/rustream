mod catalog;
mod config;
mod iceberg;
mod ingest;
mod input;
mod jobs;
mod output;
mod pg_writer;
mod reader;
mod schema;
mod state;
mod status_api;
mod sync;
mod types;
mod writer;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tokio::process::Command;
use tokio::time::{timeout, Duration};
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

    /// Create the control-plane jobs table in Postgres
    InitJobs {
        /// Control-plane Postgres connection string
        #[arg(long)]
        control_db_url: String,
    },

    /// Enqueue a single table sync job
    AddJob {
        /// Control-plane Postgres connection string
        #[arg(long)]
        control_db_url: String,

        /// Table name to sync
        #[arg(long)]
        table: String,

        /// Path to rustream config for this job
        #[arg(long)]
        config: String,

        /// Interval between runs (seconds)
        #[arg(long)]
        interval_secs: Option<i64>,

        /// Max concurrent jobs allowed for this table (default 1)
        #[arg(long)]
        max_concurrent_jobs: Option<i32>,

        /// Kill job after this many seconds (default: no timeout)
        #[arg(long)]
        timeout_secs: Option<i32>,
    },

    /// Run the worker loop that claims and executes jobs
    Worker {
        /// Control-plane Postgres connection string
        #[arg(long)]
        control_db_url: String,

        /// Poll interval when no jobs are pending (seconds)
        #[arg(long, default_value_t = 5)]
        poll_seconds: u64,

        /// Maximum jobs to execute concurrently
        #[arg(long, default_value_t = 2)]
        max_concurrent: usize,
    },

    /// Force a job to run on the next worker poll
    ForceJob {
        /// Control-plane Postgres connection string
        #[arg(long)]
        control_db_url: String,

        /// Job ID
        #[arg(long)]
        job_id: i32,
    },

    /// Serve a minimal status API (experimental)
    StatusApi {
        /// Control-plane Postgres connection string
        #[arg(long)]
        control_db_url: String,

        /// Bind address, e.g. 0.0.0.0:8080
        #[arg(long, default_value = "0.0.0.0:8080")]
        bind: String,
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
        Commands::InitJobs { control_db_url } => {
            let control_db_url = resolve_control_db_url(control_db_url)?;
            let client = jobs::connect(&control_db_url).await?;
            jobs::ensure_jobs_table(&client).await?;
            tracing::info!("ensured rustream_jobs exists");
        }
        Commands::AddJob {
            control_db_url,
            table,
            config: config_path,
            interval_secs,
            max_concurrent_jobs,
            timeout_secs,
        } => {
            let control_db_url = resolve_control_db_url(control_db_url)?;
            let client = jobs::connect(&control_db_url).await?;
            jobs::ensure_jobs_table(&client).await?;
            let id = jobs::enqueue_job(
                &client,
                &table,
                &config_path,
                interval_secs,
                max_concurrent_jobs,
                timeout_secs,
            )
            .await?;
            tracing::info!(job_id = id, table = %table, "enqueued job");
        }
        Commands::Worker {
            control_db_url,
            poll_seconds,
            max_concurrent,
        } => {
            let control_db_url = resolve_control_db_url(control_db_url)?;
            let client = jobs::connect(&control_db_url).await?;
            jobs::ensure_jobs_table(&client).await?;
            run_worker(client, poll_seconds, max_concurrent).await?;
        }
        Commands::StatusApi {
            control_db_url,
            bind,
        } => {
            let control_db_url = resolve_control_db_url(control_db_url)?;
            let addr: std::net::SocketAddr = bind.parse()?;
            status_api::serve(control_db_url, addr).await?;
        }
        Commands::ForceJob {
            control_db_url,
            job_id,
        } => {
            let control_db_url = resolve_control_db_url(control_db_url)?;
            let client = jobs::connect(&control_db_url).await?;
            jobs::ensure_jobs_table(&client).await?;
            jobs::force_run_job(&client, job_id).await?;
            tracing::info!(job_id, "job marked pending for immediate run");
        }
    }

    Ok(())
}

async fn run_worker(
    mut client: tokio_postgres::Client,
    poll_seconds: u64,
    max_concurrent: usize,
) -> Result<()> {
    if max_concurrent > 1 {
        tracing::warn!(
            max_concurrent,
            "multi-job concurrency not supported yet; running sequentially"
        );
    }

    let poll = Duration::from_secs(poll_seconds);

    loop {
        match jobs::claim_job(&mut client).await? {
            Some(job) => {
                tracing::info!(job_id = job.id, table = %job.table_name, "claimed job");
                if let Err(e) = process_job(&mut client, job).await {
                    tracing::error!(error = %e, "job execution failed");
                }
            }
            None => tokio::time::sleep(poll).await,
        }
    }
}

async fn process_job(client: &mut tokio_postgres::Client, job: jobs::Job) -> Result<()> {
    let interval = job.interval_secs as i64;
    tracing::debug!(
        job_id = job.id,
        max_concurrent = job.max_concurrent_jobs,
        timeout = job.timeout_secs,
        "starting job"
    );
    let run_id = jobs::start_job_run(client, job.id).await?;

    let sync_res = if job.timeout_secs > 0 {
        match timeout(
            Duration::from_secs(job.timeout_secs as u64),
            jobs::run_job(&job),
        )
        .await
        {
            Ok(r) => r,
            Err(_) => Err(anyhow::anyhow!(
                "job {} timed out after {}s",
                job.id,
                job.timeout_secs
            )),
        }
    } else {
        jobs::run_job(&job).await
    };

    let mut severity: Option<String> = None;
    let mut metrics_json: Option<String> = None;

    let sync_output_path = sync_res.as_ref().ok().and_then(|p| p.clone());

    let dq_res = if let Some(path) = sync_output_path {
        if let Some(cmd) = dq_command_template() {
            match run_dq_prof(&cmd, &path).await {
                Ok((sev, metrics)) => {
                    severity = sev;
                    metrics_json = metrics;
                    Ok(())
                }
                Err(e) => Err(e),
            }
        } else {
            Ok(())
        }
    } else {
        Ok(())
    };

    let final_res: Result<()> = match (sync_res, dq_res) {
        (Err(e), _) => Err(e),
        (_, Err(e)) => Err(e),
        _ => Ok(()),
    };

    let error_str = final_res.as_ref().err().map(|e| format!("{e:?}"));
    let success = final_res.is_ok();

    jobs::complete_job(client, job.id, success, interval, error_str.as_deref()).await?;
    jobs::finish_job_run(
        client,
        run_id,
        if success { "ok" } else { "failed" },
        error_str.as_deref(),
        severity.as_deref(),
        metrics_json.as_deref(),
    )
    .await?;

    if let Some(err) = error_str {
        tracing::error!(job_id = job.id, error = %err, "job failed");
    } else {
        tracing::info!(job_id = job.id, "job complete");
    }

    Ok(())
}

fn dq_command_template() -> Option<String> {
    match std::env::var("RUSTREAM_DQ_CMD") {
        Ok(s) if !s.trim().is_empty() => Some(s),
        _ => None,
    }
}

async fn run_dq_prof(cmd_template: &str, path: &str) -> Result<(Option<String>, Option<String>)> {
    let cmd = cmd_template.replace("{path}", path);
    let output = Command::new("sh").arg("-c").arg(&cmd).output().await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow::anyhow!(
            "dq-prof command failed (status {}): {}",
            output.status,
            stderr.trim()
        ));
    }

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if stdout.is_empty() {
        return Ok((None, None));
    }

    let severity = serde_json::from_str::<serde_json::Value>(&stdout)
        .ok()
        .and_then(|v| {
            v.get("severity")
                .and_then(|s| s.as_str())
                .map(|s| s.to_string())
        });

    Ok((severity, Some(stdout)))
}

fn resolve_control_db_url(flag_value: String) -> Result<String> {
    if !flag_value.is_empty() {
        return Ok(flag_value);
    }
    if let Ok(env_val) = std::env::var("RUSTREAM_CONTROL_DB_URL") {
        if !env_val.is_empty() {
            return Ok(env_val);
        }
    }
    Err(anyhow::anyhow!(
        "control-db-url is empty; set --control-db-url or RUSTREAM_CONTROL_DB_URL"
    ))
}
