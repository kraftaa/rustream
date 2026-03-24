use std::net::SocketAddr;

use anyhow::Result;
use axum::{
    extract::{Query, State},
    http::{header, StatusCode},
    response::{Redirect, Response},
    routing::get,
    Json, Router,
};
use serde::Deserialize;

use crate::jobs;

#[derive(Clone)]
struct AppState {
    control_db_url: String,
}

pub async fn serve(control_db_url: String, addr: SocketAddr) -> Result<()> {
    // Ensure schema exists before serving requests (covers older control DBs without new columns).
    let client = jobs::connect(&control_db_url).await?;
    jobs::ensure_jobs_table(&client).await?;

    let state = AppState { control_db_url };
    let app = Router::new()
        .route("/", get(root_redirect))
        .route("/jobs", get(list_jobs))
        .route("/jobs/html", get(list_jobs_html))
        .route("/jobs/summary", get(job_summary))
        .route("/logs", get(list_runs))
        .route("/health", get(health))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn root_redirect() -> Redirect {
    Redirect::temporary("/jobs/html")
}

async fn list_jobs(
    State(state): State<AppState>,
    Query(query): Query<JobsQuery>,
) -> Result<Json<Vec<jobs::JobStatus>>, StatusCode> {
    let rows = fetch_jobs(&state.control_db_url, &query).await?;
    Ok(Json(rows))
}

#[derive(Debug, Deserialize)]
pub struct JobsQuery {
    pub status: Option<String>,
    pub refresh: Option<u64>,
}

async fn list_jobs_html(
    State(state): State<AppState>,
    Query(query): Query<JobsQuery>,
) -> Result<Response, StatusCode> {
    let rows = fetch_jobs(&state.control_db_url, &query).await?;
    let summary = jobs::job_summary(&state.control_db_url)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "job summary for HTML failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let refresh_secs = query.refresh.unwrap_or(10);

    let mut body = String::new();
    body.push_str("<html><head><meta charset=\"utf-8\">");
    if refresh_secs > 0 {
        body.push_str(&format!(
            "<meta http-equiv=\"refresh\" content=\"{}\">",
            refresh_secs
        ));
    }
    body.push_str(
        "<style>
        body { font-family: Arial, sans-serif; padding: 16px; }
        table { border-collapse: collapse; width: 100%; }
        th, td { border: 1px solid #ddd; padding: 8px; }
        th { background: #f6f6f6; text-align: left; }
        tr:nth-child(even) { background: #fafafa; }
        .status-running { color: #b26b00; font-weight: 600; }
        .status-pending { color: #005ea2; }
        .status-failed { color: #c1121f; font-weight: 700; }
        .severity-ok { color: #0a8754; }
        .severity-warning { color: #b26b00; }
        .severity-error { color: #c1121f; font-weight: 700; }
        .toolbar { margin-bottom: 12px; }
        .toolbar a { margin-right: 12px; }
        </style></head><body>",
    );

    body.push_str("<h2>rustream jobs</h2>");
    body.push_str(
        "<div class=\"toolbar\">
            <a href=\"/jobs\">Jobs JSON</a>
            <a href=\"/jobs/summary\">Summary JSON</a>
            <a href=\"/logs\">Runs JSON</a>
        </div>",
    );

    body.push_str(&format!(
        "<div style=\"margin-bottom:10px;\">pending: {} · running: {} · failed: {} · total: {}</div>",
        summary.pending, summary.running, summary.failed, summary.total
    ));

    body.push_str("<form method=\"get\" action=\"/jobs/html\" class=\"toolbar\">");
    body.push_str("<label>Status filter: <select name=\"status\">");
    let statuses = vec!["", "pending", "running", "failed"];
    for s in statuses {
        let selected = query
            .status
            .as_ref()
            .map(|q| q.eq_ignore_ascii_case(s))
            .unwrap_or(false);
        let label = if s.is_empty() { "all" } else { s };
        body.push_str(&format!(
            "<option value=\"{}\" {}>{}</option>",
            s,
            if selected { "selected" } else { "" },
            label
        ));
    }
    body.push_str("</select></label>");
    body.push_str(&format!(
        "<label style=\"margin-left:12px;\">Auto-refresh (s): <input type=\"number\" name=\"refresh\" min=\"0\" max=\"300\" value=\"{}\"></label>",
        refresh_secs
    ));
    body.push_str("<button type=\"submit\">Apply</button></form>");

    body.push_str("<table><tr><th>id</th><th>table</th><th>status</th><th>next_run</th><th>last_run</th><th>last_error</th></tr>");
    for r in rows {
        let status_class = format!("status-{}", r.status.to_ascii_lowercase());
        body.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td class=\"{}\">{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            r.id,
            r.table_name,
            status_class,
            r.status,
            r.next_run.unwrap_or_else(|| "-".to_string()),
            r.last_run.unwrap_or_else(|| "-".to_string()),
            r.last_error.unwrap_or_else(|| "-".to_string()),
        ));
    }
    body.push_str("</table></body></html>");

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
        .body(body.into())
        .unwrap())
}

async fn fetch_jobs(
    control_db_url: &str,
    query: &JobsQuery,
) -> Result<Vec<jobs::JobStatus>, StatusCode> {
    let mut rows = jobs::list_job_statuses(control_db_url).await.map_err(|e| {
        tracing::error!(error = %e, "listing job statuses");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    if let Some(ref s) = query.status {
        let s_lower = s.to_ascii_lowercase();
        rows.retain(|r| r.status.to_ascii_lowercase() == s_lower);
    }

    Ok(rows)
}

async fn job_summary(State(state): State<AppState>) -> Result<Json<jobs::JobSummary>, StatusCode> {
    jobs::job_summary(&state.control_db_url)
        .await
        .map(Json)
        .map_err(|e| {
            tracing::error!(error = %e, "job summary failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })
}

#[derive(Debug, Deserialize)]
pub struct RunsQuery {
    pub limit: Option<i64>,
}

async fn list_runs(
    State(state): State<AppState>,
    Query(query): Query<RunsQuery>,
) -> Result<Json<Vec<jobs::JobRun>>, StatusCode> {
    let limit = query.limit.unwrap_or(50);
    jobs::list_job_runs(&state.control_db_url, limit)
        .await
        .map(Json)
        .map_err(|e| {
            tracing::error!(error = %e, "listing job runs failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })
}

async fn health() -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body("ok".into())
        .unwrap()
}
