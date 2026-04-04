use std::net::SocketAddr;

use anyhow::Result;
use axum::{
    extract::{Form, Query, State},
    http::{header, StatusCode},
    response::{Redirect, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;

use crate::jobs;
use crate::sync;

#[derive(Clone)]
struct AppState {
    control_db_url: String,
    state_dir: String,
}

pub async fn serve(
    control_db_url: String,
    addr: SocketAddr,
    state_dir: Option<String>,
) -> Result<()> {
    // Ensure schema exists before serving requests (covers older control DBs without new columns).
    let client = jobs::connect(&control_db_url).await?;
    jobs::ensure_jobs_table(&client).await?;

    let state = AppState {
        control_db_url,
        state_dir: state_dir.unwrap_or_else(|| ".rustream_state".to_string()),
    };
    let app = Router::new()
        .route("/", get(root_redirect))
        .route("/jobs", get(list_jobs))
        .route("/jobs/html", get(list_jobs_html))
        .route("/jobs/summary", get(job_summary))
        .route("/logs", get(list_runs))
        .route("/jobs/force", post(force_job))
        .route("/jobs/retry", post(retry_job))
        .route("/state/reset", post(reset_state))
        .route("/health/worker", get(worker_health))
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
    body.push_str(
        "<form method=\"post\" action=\"/state/reset\" style=\"margin-bottom:10px;\">
<button type=\"submit\">Reset all state</button></form>",
    );

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

    body.push_str("<table><tr><th>id</th><th>table</th><th>status</th><th>next_run</th><th>last_run</th><th>last_error</th><th>actions</th></tr>");
    for r in rows {
        let status_class = format!("status-{}", r.status.to_ascii_lowercase());
        let table_name = escape_html(&r.table_name);
        let status_text = escape_html(&r.status);
        let next_run = escape_html(&r.next_run.unwrap_or_else(|| "-".to_string()));
        let last_run = escape_html(&r.last_run.unwrap_or_else(|| "-".to_string()));
        let last_error = escape_html(&r.last_error.unwrap_or_else(|| "-".to_string()));
        body.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td class=\"{}\">{}</td><td>{}</td><td>{}</td><td>{}</td>\
            <td><form method=\"post\" action=\"/jobs/force\" style=\"margin:0;\">\
            <input type=\"hidden\" name=\"id\" value=\"{}\">\
            <button type=\"submit\">Force run</button></form>\
            {}\
            <form method=\"post\" action=\"/state/reset\" style=\"margin-top:4px;\">\
            <input type=\"hidden\" name=\"table\" value=\"{}\">\
            <button type=\"submit\">Reset state</button></form>\
            </td></tr>",
            r.id,
            table_name,
            status_class,
            status_text,
            next_run,
            last_run,
            last_error,
            r.id,
            if r.status.eq_ignore_ascii_case("failed") {
                format!(
                    "<form method=\"post\" action=\"/jobs/retry\" style=\"margin-top:4px;\">
<input type=\"hidden\" name=\"id\" value=\"{}\">
<button type=\"submit\">Retry failed</button></form>",
                    r.id
                )
            } else {
                String::new()
            },
            table_name,
        ));
    }
    body.push_str("</table>");
    // Recent runs table
    let runs = jobs::list_job_runs(&state.control_db_url, 20)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "listing runs for HTML failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    body.push_str("<h3>Recent runs</h3><table><tr><th>run_id</th><th>job_id</th><th>status</th><th>duration_ms</th><th>severity</th><th>error</th><th>started_at</th><th>finished_at</th></tr>");
    for r in runs {
        body.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            r.id,
            r.job_id,
            escape_html(&r.status),
            r.duration_ms.map(|d| d.to_string()).unwrap_or_else(|| "-".to_string()),
            escape_html(&r.severity.unwrap_or_else(|| "-".to_string())),
            escape_html(&r.error.unwrap_or_else(|| "-".to_string())),
            escape_html(&r.started_at),
            escape_html(&r.finished_at.unwrap_or_else(|| "-".to_string())),
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
    let limit = query.limit.unwrap_or(50).clamp(1, 1000);
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

async fn worker_health(State(state): State<AppState>) -> Result<Response, StatusCode> {
    let client = jobs::connect(&state.control_db_url).await.map_err(|e| {
        tracing::error!(error = %e, "health db connect failed");
        StatusCode::SERVICE_UNAVAILABLE
    })?;
    jobs::ensure_jobs_table(&client).await.map_err(|e| {
        tracing::error!(error = %e, "health ensure schema failed");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let last_run: Option<chrono::DateTime<chrono::Utc>> = client
        .query_opt(
            "SELECT finished_at FROM rustream_job_runs WHERE finished_at IS NOT NULL ORDER BY finished_at DESC LIMIT 1",
            &[],
        )
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "health query failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .and_then(|row| row.get::<_, Option<chrono::DateTime<chrono::Utc>>>(0));

    let body = serde_json::json!({
        "db": "ok",
        "last_run": last_run.map(|dt| dt.to_rfc3339()),
    });

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(serde_json::to_string(&body).unwrap().into())
        .unwrap())
}

#[derive(Debug, Deserialize)]
struct ForceJobForm {
    id: i32,
}

async fn force_job(
    State(state): State<AppState>,
    Form(form): Form<ForceJobForm>,
) -> Result<Redirect, StatusCode> {
    let client = jobs::connect(&state.control_db_url)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    jobs::ensure_jobs_table(&client)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    jobs::force_run_job(&client, form.id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Redirect::temporary("/jobs/html"))
}

async fn retry_job(
    State(state): State<AppState>,
    Form(form): Form<ForceJobForm>,
) -> Result<Redirect, StatusCode> {
    // same as force; could add status check later
    let client = jobs::connect(&state.control_db_url)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    jobs::ensure_jobs_table(&client)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    jobs::force_run_job(&client, form.id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Redirect::temporary("/jobs/html"))
}

#[derive(Debug, Deserialize)]
struct ResetForm {
    table: Option<String>,
}

async fn reset_state(
    State(state): State<AppState>,
    Form(form): Form<ResetForm>,
) -> Result<Redirect, StatusCode> {
    sync::reset_state(Some(state.state_dir.clone()), form.table.clone())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Redirect::temporary("/jobs/html"))
}

fn escape_html(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}
