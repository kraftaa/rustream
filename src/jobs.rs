use anyhow::{anyhow, Context, Result};
use chrono::{Duration, Utc};
use serde::Serialize;
use tokio_postgres::{Client, NoTls, Row};

use crate::config::TableConfig;

pub const DEFAULT_INTERVAL_SECS: i64 = 300;

#[derive(Debug, Clone)]
pub struct Job {
    pub id: i32,
    pub table_name: String,
    pub config_path: String,
    pub interval_secs: i32,
    pub max_concurrent_jobs: i32,
    pub timeout_secs: i32,
}

#[derive(Debug, Serialize)]
pub struct JobStatus {
    pub id: i32,
    pub table_name: String,
    pub status: String,
    pub next_run: Option<String>,
    pub last_run: Option<String>,
    pub last_error: Option<String>,
}

pub async fn connect(control_db_url: &str) -> Result<Client> {
    let (client, connection) = tokio_postgres::connect(control_db_url, NoTls)
        .await
        .context("connecting to control plane Postgres")?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!(error = %e, "control plane connection error");
        }
    });

    Ok(client)
}

pub async fn ensure_jobs_table(client: &Client) -> Result<()> {
    client
        .execute(
            "CREATE TABLE IF NOT EXISTS rustream_jobs (
                id SERIAL PRIMARY KEY,
                table_name TEXT NOT NULL,
                config_path TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                next_run TIMESTAMPTZ NOT NULL DEFAULT now(),
                last_run TIMESTAMPTZ,
                last_error TEXT,
                interval_secs INTEGER NOT NULL DEFAULT 300,
                max_concurrent_jobs INTEGER NOT NULL DEFAULT 1,
                timeout_secs INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )",
            &[],
        )
        .await
        .context("creating rustream_jobs table")?;

    // Backfill columns for existing deployments (idempotent on recent Postgres versions)
    client
        .execute(
            "ALTER TABLE rustream_jobs ADD COLUMN IF NOT EXISTS max_concurrent_jobs INTEGER NOT NULL DEFAULT 1",
            &[],
        )
        .await
        .context("adding max_concurrent_jobs column")?;
    client
        .execute(
            "ALTER TABLE rustream_jobs ADD COLUMN IF NOT EXISTS timeout_secs INTEGER NOT NULL DEFAULT 0",
            &[],
        )
        .await
        .context("adding timeout_secs column")?;

    client
        .execute(
            "CREATE TABLE IF NOT EXISTS rustream_job_runs (
                id SERIAL PRIMARY KEY,
                job_id INTEGER NOT NULL REFERENCES rustream_jobs(id) ON DELETE CASCADE,
                started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                finished_at TIMESTAMPTZ,
                status TEXT NOT NULL,
                error TEXT,
                severity TEXT,
                metrics_json TEXT
            )",
            &[],
        )
        .await
        .context("creating rustream_job_runs table")?;

    // Backfill columns for existing deployments
    client
        .execute(
            "ALTER TABLE rustream_job_runs ADD COLUMN IF NOT EXISTS severity TEXT",
            &[],
        )
        .await
        .context("adding severity column to rustream_job_runs")?;
    client
        .execute(
            "ALTER TABLE rustream_job_runs ADD COLUMN IF NOT EXISTS metrics_json TEXT",
            &[],
        )
        .await
        .context("adding metrics_json column to rustream_job_runs")?;

    Ok(())
}

pub async fn enqueue_job(
    client: &Client,
    table_name: &str,
    config_path: &str,
    interval_secs: Option<i64>,
    max_concurrent_jobs: Option<i32>,
    timeout_secs: Option<i32>,
) -> Result<i32> {
    let interval = interval_secs.unwrap_or(DEFAULT_INTERVAL_SECS) as i32;
    let max_concurrent_jobs = max_concurrent_jobs.unwrap_or(1);
    let timeout_secs = timeout_secs.unwrap_or(0);
    let row = client
        .query_one(
            "INSERT INTO rustream_jobs (table_name, config_path, interval_secs, max_concurrent_jobs, timeout_secs)
             VALUES ($1, $2, $3, $4, $5)
             RETURNING id",
            &[&table_name, &config_path, &interval, &max_concurrent_jobs, &timeout_secs],
        )
        .await
        .context("inserting job")?;
    Ok(row.get(0))
}

pub async fn claim_job(client: &mut Client) -> Result<Option<Job>> {
    let tx = client
        .transaction()
        .await
        .context("starting claim transaction")?;

    let row = tx
        .query_opt(
            "SELECT id, table_name, config_path, interval_secs, max_concurrent_jobs, timeout_secs
             FROM rustream_jobs
             WHERE status = 'pending' AND next_run <= now()
             ORDER BY next_run
             FOR UPDATE SKIP LOCKED
             LIMIT 1",
            &[],
        )
        .await
        .context("querying pending job")?;

    let Some(r) = row else {
        tx.rollback().await.ok();
        return Ok(None);
    };
    let job = row_to_job(&r);

    tx.execute(
        "UPDATE rustream_jobs
         SET status = 'running', updated_at = now(), last_error = NULL
         WHERE id = $1",
        &[&job.id],
    )
    .await
    .context("marking job running")?;
    tx.commit().await.context("committing claim")?;

    Ok(Some(job))
}

pub async fn complete_job(
    client: &Client,
    job_id: i32,
    success: bool,
    interval_secs: i64,
    error: Option<&str>,
) -> Result<()> {
    let status = if success { "pending" } else { "failed" };
    let next = Utc::now() + Duration::seconds(interval_secs);

    client
        .execute(
            "UPDATE rustream_jobs
             SET status = $1,
                 next_run = $2,
                 last_run = now(),
                 last_error = $3,
                 updated_at = now()
             WHERE id = $4",
            &[&status, &next, &error, &job_id],
        )
        .await
        .context("updating job status")?;
    Ok(())
}

pub async fn list_job_statuses(control_db_url: &str) -> Result<Vec<JobStatus>> {
    let client = connect(control_db_url).await?;
    let rows = client
        .query(
            "SELECT id, table_name, status, next_run, last_run, last_error
             FROM rustream_jobs
             ORDER BY next_run ASC NULLS LAST",
            &[],
        )
        .await
        .context("listing job statuses")?;

    let mut result = Vec::new();
    for r in rows {
        result.push(JobStatus {
            id: r.get(0),
            table_name: r.get(1),
            status: r.get(2),
            next_run: r
                .get::<_, Option<chrono::DateTime<chrono::Utc>>>(3)
                .map(|dt| dt.to_rfc3339()),
            last_run: r
                .get::<_, Option<chrono::DateTime<chrono::Utc>>>(4)
                .map(|dt| dt.to_rfc3339()),
            last_error: r.get(5),
        });
    }
    Ok(result)
}

pub async fn start_job_run(client: &Client, job_id: i32) -> Result<i32> {
    let row = client
        .query_one(
            "INSERT INTO rustream_job_runs (job_id, status)
             VALUES ($1, 'running')
             RETURNING id",
            &[&job_id],
        )
        .await
        .context("inserting job run")?;
    Ok(row.get(0))
}

pub async fn finish_job_run(
    client: &Client,
    run_id: i32,
    status: &str,
    error: Option<&str>,
    severity: Option<&str>,
    metrics_json: Option<&str>,
) -> Result<()> {
    client
        .execute(
            "UPDATE rustream_job_runs
             SET status = $1, error = $2, severity = $3, metrics_json = $4, finished_at = now()
             WHERE id = $5",
            &[&status, &error, &severity, &metrics_json, &run_id],
        )
        .await
        .context("updating job run")?;
    Ok(())
}

pub async fn force_run_job(client: &Client, job_id: i32) -> Result<()> {
    client
        .execute(
            "UPDATE rustream_jobs
             SET status = 'pending', next_run = now(), updated_at = now()
             WHERE id = $1",
            &[&job_id],
        )
        .await
        .context("forcing job run")?;
    Ok(())
}

#[derive(Debug, Serialize)]
pub struct JobRun {
    pub id: i32,
    pub job_id: i32,
    pub status: String,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub error: Option<String>,
    pub severity: Option<String>,
    pub metrics_json: Option<String>,
}

pub async fn list_job_runs(control_db_url: &str, limit: i64) -> Result<Vec<JobRun>> {
    let client = connect(control_db_url).await?;
    let rows = client
        .query(
            "SELECT id, job_id, status, started_at, finished_at, error, severity, metrics_json
             FROM rustream_job_runs
             ORDER BY started_at DESC
             LIMIT $1",
            &[&limit],
        )
        .await
        .context("listing job runs")?;

    let mut out = Vec::new();
    for r in rows {
        let started: chrono::DateTime<chrono::Utc> = r.get(3);
        let finished = r.get::<_, Option<chrono::DateTime<chrono::Utc>>>(4);
        out.push(JobRun {
            id: r.get(0),
            job_id: r.get(1),
            status: r.get(2),
            started_at: started.to_rfc3339(),
            finished_at: finished.map(|dt| dt.to_rfc3339()),
            error: r.get(5),
            severity: r.get(6),
            metrics_json: r.get(7),
        });
    }
    Ok(out)
}

#[derive(Debug, Serialize)]
pub struct JobSummary {
    pub pending: i64,
    pub running: i64,
    pub failed: i64,
    pub total: i64,
}

pub async fn job_summary(control_db_url: &str) -> Result<JobSummary> {
    let client = connect(control_db_url).await?;
    let rows = client
        .query(
            "SELECT status, count(*) FROM rustream_jobs GROUP BY status",
            &[],
        )
        .await
        .context("summarizing jobs")?;

    let mut summary = JobSummary {
        pending: 0,
        running: 0,
        failed: 0,
        total: 0,
    };
    for r in rows {
        let status: String = r.get(0);
        let count: i64 = r.get(1);
        summary.total += count;
        match status.to_ascii_lowercase().as_str() {
            "pending" => summary.pending += count,
            "running" => summary.running += count,
            "failed" => summary.failed += count,
            _ => {}
        }
    }
    Ok(summary)
}

fn row_to_job(row: &Row) -> Job {
    Job {
        id: row.get(0),
        table_name: row.get(1),
        config_path: row.get(2),
        interval_secs: row.get(3),
        max_concurrent_jobs: row.get(4),
        timeout_secs: row.get(5),
    }
}

/// Run a single job by loading its config and overriding tables to the target table.
/// Returns local output path for the table if output is local.
pub async fn run_job(job: &Job) -> Result<Option<String>> {
    let mut cfg = crate::config::load(&job.config_path)?;

    // Preserve the original table config if present; otherwise, synthesize a minimal entry.
    let maybe_table = cfg
        .tables
        .unwrap_or_default()
        .iter()
        .find(|t| t.name == job.table_name)
        .cloned();

    cfg.tables = Some(vec![maybe_table.unwrap_or(TableConfig {
        name: job.table_name.clone(),
        schema: None,
        columns: None,
        incremental_column: None,
        incremental_tiebreaker_column: None,
        incremental_column_is_unique: false,
        partition_by: None,
    })]);

    let local_output = match &cfg.output {
        Some(crate::config::OutputConfig::Local { path }) => {
            Some(format!("{}/{}", path, job.table_name))
        }
        _ => None,
    };

    crate::sync::run(cfg).await.map_err(|e| {
        anyhow!(
            "job {} (table {}): sync failed: {}",
            job.id,
            job.table_name,
            e
        )
    })?;

    Ok(local_output)
}
