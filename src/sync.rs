use anyhow::{anyhow, Context, Result};
use arrow::record_batch::RecordBatch;
use chrono::Utc;
use iceberg::Catalog;
use std::sync::Arc;
use tokio_postgres::NoTls;

use crate::catalog;
use crate::config::{Config, OutputFormat, PartitionBy, TableConfig};
use crate::iceberg as iceberg_write;
use crate::output;
use crate::reader;
use crate::schema::{self, ColumnInfo};
use crate::state::StateStore;
use crate::writer;

pub(crate) async fn connect(config: &Config) -> Result<tokio_postgres::Client> {
    let conn_str = config.postgres.connection_string();
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .context("connecting to Postgres")?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!(error = %e, "Postgres connection error");
        }
    });

    tracing::info!(
        host = %config.postgres.host,
        database = %config.postgres.database,
        "connected to Postgres"
    );

    Ok(client)
}

async fn resolve_tables(
    config: &Config,
    client: &tokio_postgres::Client,
) -> Result<Vec<TableConfig>> {
    match &config.tables {
        Some(tables) => Ok(tables.clone()),
        None => {
            let discovered = schema::discover_tables(client, &config.schema).await?;
            tracing::info!(schema = %config.schema, count = discovered.len(), "discovered tables");

            let tables = discovered
                .into_iter()
                .filter(|t| !config.exclude.contains(t))
                .map(|name| TableConfig {
                    name,
                    schema: Some(config.schema.clone()),
                    columns: None,
                    incremental_column: None,
                    incremental_tiebreaker_column: None,
                    incremental_column_is_unique: false,
                    partition_by: None,
                })
                .collect();
            Ok(tables)
        }
    }
}

/// Dry run: connect, resolve tables, show what would be synced.
pub async fn dry_run(config: Config) -> Result<()> {
    let client = connect(&config).await?;
    let tables = resolve_tables(&config, &client).await?;

    println!("Would sync {} tables:\n", tables.len());

    for table in &tables {
        let columns = schema::introspect_table(&client, table).await?;
        let col_names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
        let mode = if table.incremental_column.is_some() {
            if table.incremental_tiebreaker_column.is_some() {
                "incremental (cursor)"
            } else if table.incremental_column_is_unique {
                "incremental (unique watermark)"
            } else {
                "incremental (invalid config: missing tiebreaker)"
            }
        } else {
            "full"
        };
        let partition = match &table.partition_by {
            Some(PartitionBy::Date) => ", partitioned by date",
            Some(PartitionBy::Month) => ", partitioned by month",
            Some(PartitionBy::Year) => ", partitioned by year",
            None => "",
        };

        println!(
            "  {} ({} columns, {} sync{})",
            table.full_name(),
            columns.len(),
            mode,
            partition
        );
        if !columns.is_empty() {
            println!("    columns: {}", col_names.join(", "));
        } else {
            println!("    (no columns visible — check permissions)");
        }
    }

    println!(
        "\nOutput: {:?}",
        config
            .output
            .as_ref()
            .expect("output config required for sync")
    );

    Ok(())
}

/// Run the sync process for all configured tables.
pub async fn run(config: Config) -> Result<()> {
    let client = connect(&config).await?;
    let tables = resolve_tables(&config, &client).await?;

    tracing::info!(tables = tables.len(), "syncing tables");

    let state_dir = config
        .state_dir
        .clone()
        .unwrap_or_else(|| ".rustream_state".to_string());
    let state = StateStore::open(&state_dir)?;

    // Build Iceberg catalog if format is iceberg
    let iceberg_catalog: Option<Arc<dyn Catalog>> = if config.format == OutputFormat::Iceberg {
        let warehouse = config.warehouse.as_deref().expect("validated at load");
        let cat = catalog::build_catalog(warehouse, config.catalog.as_ref()).await?;
        Some(cat)
    } else {
        None
    };

    let mut failures = Vec::new();
    let fail_on_empty_columns = config.tables.is_some();
    for table in &tables {
        if let Err(e) = sync_table(
            &client,
            &config,
            table,
            &state,
            fail_on_empty_columns,
            iceberg_catalog.as_ref(),
        )
        .await
        {
            tracing::error!(table = %table.full_name(), error = ?e, "failed to sync table");
            failures.push(format!("{}: {e}", table.full_name()));
        }
    }

    if !failures.is_empty() {
        return Err(anyhow!(
            "{} table(s) failed during sync: {}",
            failures.len(),
            failures.join(" | ")
        ));
    }

    tracing::info!("sync complete");
    Ok(())
}

/// Optionally clear all saved state (watermarks/cursors) before a run.
pub fn maybe_reset_state(config: &Config, reset_state: bool) -> Result<()> {
    if !reset_state {
        return Ok(());
    }

    let state_dir = config
        .state_dir
        .clone()
        .unwrap_or_else(|| ".rustream_state".to_string());
    let path = std::path::Path::new(&state_dir).join("rustream_state.db");
    if path.exists() {
        std::fs::remove_file(&path)
            .with_context(|| format!("removing state db at {}", path.display()))?;
        tracing::warn!("reset state: deleted {}", path.display());
    }
    Ok(())
}

/// Reset state for all tables or a specific table.
pub fn reset_state(state_dir: Option<String>, table: Option<String>) -> Result<()> {
    let dir = state_dir.unwrap_or_else(|| ".rustream_state".to_string());
    let path = std::path::Path::new(&dir).join("rustream_state.db");
    if !path.exists() {
        tracing::info!(state_dir = %dir, "state db not found; nothing to reset");
        return Ok(());
    }

    if let Some(table_name) = table {
        let store = StateStore::open(&dir)?;
        let deleted = store.clear_progress_by_table_ref(&table_name)?;
        tracing::info!(table = %table_name, deleted, "cleared state for table reference");
    } else {
        let store = StateStore::open(&dir)?;
        store.clear_all_progress()?;
        tracing::info!(state_file = %path.display(), "cleared all state");
    }
    Ok(())
}

async fn sync_table(
    client: &tokio_postgres::Client,
    config: &Config,
    table: &TableConfig,
    state: &StateStore,
    fail_on_empty_columns: bool,
    iceberg_catalog: Option<&Arc<dyn Catalog>>,
) -> Result<()> {
    let table_name = table.full_name();
    tracing::info!(table = %table_name, "starting sync");

    // Introspect schema
    let columns = schema::introspect_table(client, table).await?;
    if columns.is_empty() {
        if fail_on_empty_columns {
            return Err(anyhow!(
                "table {} has no visible columns (table missing or no privileges)",
                table_name
            ));
        }
        tracing::warn!(table = %table_name, "no columns found, skipping");
        return Ok(());
    }

    let arrow_schema = schema::build_arrow_schema(&columns);
    tracing::info!(
        table = %table_name,
        columns = columns.len(),
        "introspected schema"
    );

    // Get watermark info
    let watermark_col = table.incremental_column.as_deref();
    let cursor_col = watermark_col.and(table.incremental_tiebreaker_column.as_deref());

    if watermark_col.is_some() && cursor_col.is_none() && !table.incremental_column_is_unique {
        return Err(anyhow!(
            "incremental_tiebreaker_column is required for incremental table {} unless incremental_column_is_unique=true",
            table_name
        ));
    }

    if let (Some(wm), Some(cur)) = (watermark_col, cursor_col) {
        if wm == cur {
            return Err(anyhow!(
                "incremental_tiebreaker_column '{}' cannot equal incremental_column '{}' for table {}",
                cur,
                wm,
                table_name
            ));
        }
    }

    if let Some(wm) = watermark_col {
        if !columns.iter().any(|c| c.name == wm) {
            return Err(anyhow!(
                "incremental_column '{}' is not in selected columns for table {}",
                wm,
                table_name
            ));
        }
    }
    if let Some(cur) = cursor_col {
        if !columns.iter().any(|c| c.name == cur) {
            return Err(anyhow!(
                "incremental_tiebreaker_column '{}' is not in selected columns for table {}",
                cur,
                table_name
            ));
        }
    }

    if let (Some(wm), Some(cur)) = (watermark_col, cursor_col) {
        tracing::info!(table = %table_name, watermark_col = %wm, cursor_col = %cur, "incremental cursor enabled");
    } else if let Some(wm) = watermark_col {
        tracing::info!(
            table = %table_name,
            watermark_col = %wm,
            "incremental watermark-only mode enabled (column marked unique)"
        );
    }

    let (mut watermark_val, mut cursor_val) = match watermark_col {
        Some(_) => match state.get_progress(&table_name)? {
            Some((wm, cursor)) => {
                if cursor_col.is_some() && cursor.is_none() {
                    return Err(anyhow!(
                        "state for table {} has watermark but no cursor; reset this table state and rerun",
                        table_name
                    ));
                }
                (Some(wm), cursor)
            }
            None => (None, None),
        },
        None => (None, None),
    };

    // Clear invalid stored progress (e.g., cursor stored as text when column is int).
    if let Some(wm) = &watermark_val {
        if !schema::value_fits_column(wm, watermark_col.unwrap(), &columns) {
            tracing::warn!(table = %table_name, value = %wm, "clearing invalid stored watermark");
            state.clear_progress(&table_name)?;
            watermark_val = None;
            cursor_val = None;
        }
    }
    if let (Some(cur_col), Some(cur_val)) = (cursor_col, &cursor_val) {
        if !schema::value_fits_column(cur_val, cur_col, &columns) {
            tracing::warn!(table = %table_name, value = %cur_val, "clearing invalid stored cursor");
            state.clear_progress(&table_name)?;
            watermark_val = None;
            cursor_val = None;
        }
    }

    let mut total_rows = 0u64;
    let mut batch_num = 0u32;
    // Collect batches for Iceberg (needs all batches for a single commit)
    let mut iceberg_batches: Vec<RecordBatch> = Vec::new();

    loop {
        let batch = reader::read_batch(
            client,
            table,
            &columns,
            &arrow_schema,
            reader::ReadBatchOptions {
                watermark_col,
                cursor_col,
                watermark_val: watermark_val.as_deref(),
                cursor_val: cursor_val.as_deref(),
                batch_size: config.batch_size,
            },
        )
        .await?;

        let batch = match batch {
            Some(b) if b.num_rows() > 0 => b,
            _ => break,
        };

        let num_rows = batch.num_rows();
        total_rows += num_rows as u64;

        let new_watermark = match watermark_col {
            Some(col) => Some(extract_watermark(&columns, &batch, col).ok_or_else(|| {
                anyhow!(
                    "incremental column '{}' produced no watermark value for table {}",
                    col,
                    table_name
                )
            })?),
            None => None,
        };
        let new_cursor = match cursor_col {
            Some(col) => Some(extract_watermark(&columns, &batch, col).ok_or_else(|| {
                anyhow!(
                    "incremental tiebreaker column '{}' produced no cursor value for table {}",
                    col,
                    table_name
                )
            })?),
            None => None,
        };

        match config.format {
            OutputFormat::Parquet => {
                // Write parquet to buffer
                let mut buf = Vec::new();
                writer::write_parquet(&mut buf, &[batch])?;

                // Generate output filename
                let now = Utc::now();
                let filename = match &table.partition_by {
                    Some(PartitionBy::Date) => format!(
                        "{}/year={}/month={:02}/day={:02}/{}_{:04}.parquet",
                        table.name,
                        now.format("%Y"),
                        now.format("%m"),
                        now.format("%d"),
                        now.format("%H%M%S"),
                        batch_num
                    ),
                    Some(PartitionBy::Month) => format!(
                        "{}/year={}/month={:02}/{}_{:04}.parquet",
                        table.name,
                        now.format("%Y"),
                        now.format("%m"),
                        now.format("%d_%H%M%S"),
                        batch_num
                    ),
                    Some(PartitionBy::Year) => format!(
                        "{}/year={}/{}_{:04}.parquet",
                        table.name,
                        now.format("%Y"),
                        now.format("%m%d_%H%M%S"),
                        batch_num
                    ),
                    None => format!(
                        "{}/{}_{:04}.parquet",
                        table.name,
                        now.format("%Y%m%d_%H%M%S"),
                        batch_num
                    ),
                };

                let output = config
                    .output
                    .as_ref()
                    .expect("output config required for sync");
                output::write_output(output, &filename, buf).await?;
            }
            OutputFormat::Iceberg => {
                iceberg_batches.push(batch);
            }
        }

        // Always advance in-memory cursor for next batch query.
        if let Some(ref wm) = new_watermark {
            watermark_val = Some(wm.clone());
            cursor_val = new_cursor.clone();

            // For Parquet we can checkpoint immediately after each successful file write.
            if config.format == OutputFormat::Parquet {
                state.set_progress(&table_name, wm, new_cursor.as_deref())?;
                tracing::debug!(
                    table = %table_name,
                    watermark = %wm,
                    cursor = ?cursor_val,
                    "checkpointed watermark"
                );
            }
        }

        batch_num += 1;

        // If we got fewer rows than batch_size, we're done
        if num_rows < config.batch_size {
            break;
        }
    }

    // For Iceberg format, write all batches in a single commit
    if config.format == OutputFormat::Iceberg && !iceberg_batches.is_empty() {
        let catalog = iceberg_catalog.expect("iceberg catalog required for iceberg format");
        iceberg_write::write_iceberg(catalog, &table_name, &iceberg_batches).await?;
        if let Some(ref wm) = watermark_val {
            state.set_progress(&table_name, wm, cursor_val.as_deref())?;
            tracing::debug!(
                table = %table_name,
                watermark = %wm,
                cursor = ?cursor_val,
                "checkpointed watermark after iceberg commit"
            );
        }
    }

    if let Some(ref wm) = watermark_val {
        tracing::info!(table = %table_name, watermark = %wm, "final watermark");
    }

    tracing::info!(table = %table_name, rows = total_rows, files = batch_num, "sync complete");
    Ok(())
}

/// Extract the watermark value from the last row of a batch.
pub(crate) fn extract_watermark(
    columns: &[ColumnInfo],
    batch: &arrow::record_batch::RecordBatch,
    watermark_col: &str,
) -> Option<String> {
    let col_idx = columns.iter().position(|c| c.name == watermark_col)?;
    let array = batch.column(col_idx);
    let last_row = batch.num_rows() - 1;

    use arrow::array::*;
    use arrow::datatypes::{DataType, TimeUnit};

    match array.data_type() {
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr = array.as_any().downcast_ref::<TimestampMicrosecondArray>()?;
            if arr.is_null(last_row) {
                return None;
            }
            let micros = arr.value(last_row);
            let dt = chrono::DateTime::from_timestamp_micros(micros)?;
            Some(dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>()?;
            if arr.is_null(last_row) {
                return None;
            }
            Some(arr.value(last_row).to_string())
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>()?;
            if arr.is_null(last_row) {
                return None;
            }
            Some(arr.value(last_row).to_string())
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>()?;
            if arr.is_null(last_row) {
                return None;
            }
            Some(arr.value(last_row).to_string())
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>()?;
            if arr.is_null(last_row) {
                return None;
            }
            Some(arr.value(last_row).to_string())
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>()?;
            if arr.is_null(last_row) {
                return None;
            }
            Some(arr.value(last_row).to_string())
        }
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>()?;
            if arr.is_null(last_row) {
                return None;
            }
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?;
            let d = epoch + chrono::Duration::days(arr.value(last_row) as i64);
            Some(d.to_string())
        }
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>()?;
            if arr.is_null(last_row) {
                return None;
            }
            Some(arr.value(last_row).to_string())
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>()?;
            if arr.is_null(last_row) {
                return None;
            }
            Some(arr.value(last_row).to_string())
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{OutputConfig, PostgresConfig};
    use crate::reader;
    use crate::schema;
    use crate::state::StateStore;
    use anyhow::Result;
    use arrow::array::{Int32Array, Int64Array, StringArray, TimestampMicrosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio_postgres::NoTls;

    #[test]
    fn extract_watermark_from_timestamp() {
        let columns = vec![ColumnInfo {
            name: "updated_at".into(),
            pg_type: "timestamptz".into(),
            arrow_type: DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            is_nullable: true,
        }];

        let schema = Arc::new(Schema::new(vec![Field::new(
            "updated_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        )]));

        // 2026-02-10 12:00:00 UTC in microseconds
        let micros = 1_770_681_600_000_000i64;
        let arr = TimestampMicrosecondArray::from(vec![Some(micros)]).with_timezone("UTC");
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

        let wm = extract_watermark(&columns, &batch, "updated_at");
        assert!(wm.is_some());
        assert!(wm.unwrap().starts_with("2026-02-10"));
    }

    #[test]
    fn extract_watermark_from_int64() {
        let columns = vec![ColumnInfo {
            name: "id".into(),
            pg_type: "bigint".into(),
            arrow_type: DataType::Int64,
            is_nullable: false,
        }];

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let arr = Int64Array::from(vec![100, 200, 300]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

        let wm = extract_watermark(&columns, &batch, "id");
        assert_eq!(wm, Some("300".to_string()));
    }

    #[test]
    fn extract_watermark_from_int32() {
        let columns = vec![ColumnInfo {
            name: "id".into(),
            pg_type: "integer".into(),
            arrow_type: DataType::Int32,
            is_nullable: false,
        }];

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let arr = Int32Array::from(vec![10, 20, 30]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

        let wm = extract_watermark(&columns, &batch, "id");
        assert_eq!(wm, Some("30".to_string()));
    }

    #[test]
    fn extract_watermark_from_utf8() {
        let columns = vec![ColumnInfo {
            name: "version".into(),
            pg_type: "text".into(),
            arrow_type: DataType::Utf8,
            is_nullable: true,
        }];

        let schema = Arc::new(Schema::new(vec![Field::new(
            "version",
            DataType::Utf8,
            true,
        )]));
        let arr = StringArray::from(vec!["v1", "v2", "v3"]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

        let wm = extract_watermark(&columns, &batch, "version");
        assert_eq!(wm, Some("v3".to_string()));
    }

    #[test]
    fn extract_watermark_missing_column() {
        let columns = vec![ColumnInfo {
            name: "id".into(),
            pg_type: "integer".into(),
            arrow_type: DataType::Int32,
            is_nullable: false,
        }];

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let arr = arrow::array::Int32Array::from(vec![1]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

        let wm = extract_watermark(&columns, &batch, "nonexistent");
        assert_eq!(wm, None);
    }

    #[test]
    fn extract_watermark_null_value() {
        let columns = vec![ColumnInfo {
            name: "updated_at".into(),
            pg_type: "bigint".into(),
            arrow_type: DataType::Int64,
            is_nullable: true,
        }];

        let schema = Arc::new(Schema::new(vec![Field::new(
            "updated_at",
            DataType::Int64,
            true,
        )]));
        let arr = Int64Array::from(vec![None::<i64>]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

        let wm = extract_watermark(&columns, &batch, "updated_at");
        assert_eq!(wm, None);
    }

    /// Confirms sync resumes from saved progress and writes only remaining rows.
    #[tokio::test]
    async fn sync_table_resumes_from_saved_progress() -> Result<()> {
        let db_url = match std::env::var("RUSTREAM_IT_DB_URL") {
            Ok(v) => v,
            Err(_) => return Ok(()), // Optional integration-style test.
        };

        let (client, connection) = tokio_postgres::connect(&db_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let table_name = format!("rustream_it_sync_resume_{suffix}");
        let full_table = format!("public.{table_name}");

        client
            .execute(
                &format!(
                    "CREATE TABLE {full_table} (
                        id INTEGER PRIMARY KEY,
                        updated_at TIMESTAMPTZ NOT NULL,
                        payload TEXT
                    )"
                ),
                &[],
            )
            .await?;

        client
            .execute(
                &format!(
                    "INSERT INTO {full_table} (id, updated_at, payload) VALUES
                    (1, '2026-01-01T00:00:00Z', 'a'),
                    (2, '2026-01-01T00:00:00Z', 'b'),
                    (3, '2026-01-01T00:00:00Z', 'c'),
                    (4, '2026-01-01T00:01:00Z', 'd')"
                ),
                &[],
            )
            .await?;

        let tmp = tempfile::tempdir()?;
        let out_dir = tmp.path().join("out");
        let state_dir = tmp.path().join("state");
        std::fs::create_dir_all(&out_dir)?;
        std::fs::create_dir_all(&state_dir)?;

        let table = TableConfig {
            name: table_name.clone(),
            schema: Some("public".to_string()),
            columns: None,
            incremental_column: Some("updated_at".to_string()),
            incremental_tiebreaker_column: Some("id".to_string()),
            incremental_column_is_unique: false,
            partition_by: None,
        };

        // Derive a realistic saved progress point from first page (ids 1,2).
        let columns = schema::introspect_table(&client, &table).await?;
        let arrow_schema = schema::build_arrow_schema(&columns);
        let first_batch = reader::read_batch(
            &client,
            &table,
            &columns,
            &arrow_schema,
            reader::ReadBatchOptions {
                watermark_col: Some("updated_at"),
                cursor_col: Some("id"),
                watermark_val: None,
                cursor_val: None,
                batch_size: 2,
            },
        )
        .await?
        .expect("first batch expected");

        let saved_wm = extract_watermark(&columns, &first_batch, "updated_at").unwrap();
        let saved_cursor = extract_watermark(&columns, &first_batch, "id").unwrap();

        let state = StateStore::open(state_dir.to_str().unwrap())?;
        state.set_progress(&full_table, &saved_wm, Some(&saved_cursor))?;

        let config = Config {
            postgres: PostgresConfig {
                host: "localhost".to_string(),
                port: 5432,
                database: "ignored_for_sync_table_test".to_string(),
                user: "ignored".to_string(),
                password: None,
            },
            output: Some(OutputConfig::Local {
                path: out_dir.to_string_lossy().to_string(),
            }),
            tables: None,
            exclude: vec![],
            schema: "public".to_string(),
            batch_size: 2,
            state_dir: Some(state_dir.to_string_lossy().to_string()),
            format: OutputFormat::Parquet,
            catalog: None,
            warehouse: None,
            ingest: None,
        };

        sync_table(&client, &config, &table, &state, false, None).await?;

        // Validate only remaining ids (3,4) are written.
        let mut seen_ids = Vec::new();
        for entry in std::fs::read_dir(out_dir.join(&table_name))? {
            let path = entry?.path();
            if path.extension().and_then(|s| s.to_str()) != Some("parquet") {
                continue;
            }
            let file = File::open(path)?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
            let reader = builder.build()?;
            for batch in reader {
                let batch = batch?;
                let id_idx = batch
                    .schema()
                    .fields()
                    .iter()
                    .position(|f| f.name() == "id")
                    .unwrap();
                let arr = batch
                    .column(id_idx)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                for i in 0..arr.len() {
                    seen_ids.push(arr.value(i));
                }
            }
        }
        seen_ids.sort_unstable();
        assert_eq!(seen_ids, vec![3, 4]);

        let progress = state.get_progress(&full_table)?.unwrap();
        assert_eq!(progress.1.as_deref(), Some("4"));

        client
            .execute(&format!("DROP TABLE {full_table}"), &[])
            .await?;

        Ok(())
    }

    /// Ensures cursor mode fails fast when saved state is missing cursor_value.
    #[tokio::test]
    async fn sync_table_fails_when_cursor_state_missing_in_cursor_mode() -> Result<()> {
        let db_url = match std::env::var("RUSTREAM_IT_DB_URL") {
            Ok(v) => v,
            Err(_) => return Ok(()), // Optional integration-style test.
        };

        let (client, connection) = tokio_postgres::connect(&db_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let table_name = format!("rustream_it_sync_missing_cursor_{suffix}");
        let full_table = format!("public.{table_name}");

        client
            .execute(
                &format!(
                    "CREATE TABLE {full_table} (
                        id INTEGER PRIMARY KEY,
                        updated_at TIMESTAMPTZ NOT NULL
                    )"
                ),
                &[],
            )
            .await?;

        client
            .execute(
                &format!(
                    "INSERT INTO {full_table} (id, updated_at) VALUES
                    (1, '2026-01-01T00:00:00Z'),
                    (2, '2026-01-01T00:00:00Z')"
                ),
                &[],
            )
            .await?;

        let tmp = tempfile::tempdir()?;
        let out_dir = tmp.path().join("out");
        let state_dir = tmp.path().join("state");
        std::fs::create_dir_all(&out_dir)?;
        std::fs::create_dir_all(&state_dir)?;

        let table = TableConfig {
            name: table_name.clone(),
            schema: Some("public".to_string()),
            columns: None,
            incremental_column: Some("updated_at".to_string()),
            incremental_tiebreaker_column: Some("id".to_string()),
            incremental_column_is_unique: false,
            partition_by: None,
        };

        let state = StateStore::open(state_dir.to_str().unwrap())?;
        // Seed legacy-like state with watermark only, no cursor.
        state.set_progress(&full_table, "2026-01-01 00:00:00.000000", None)?;

        let config = Config {
            postgres: PostgresConfig {
                host: "localhost".to_string(),
                port: 5432,
                database: "ignored_for_sync_table_test".to_string(),
                user: "ignored".to_string(),
                password: None,
            },
            output: Some(OutputConfig::Local {
                path: out_dir.to_string_lossy().to_string(),
            }),
            tables: None,
            exclude: vec![],
            schema: "public".to_string(),
            batch_size: 2,
            state_dir: Some(state_dir.to_string_lossy().to_string()),
            format: OutputFormat::Parquet,
            catalog: None,
            warehouse: None,
            ingest: None,
        };

        let err = sync_table(&client, &config, &table, &state, false, None)
            .await
            .expect_err("cursor mode should fail when saved cursor is missing");
        assert!(err.to_string().contains("has watermark but no cursor"));

        client
            .execute(&format!("DROP TABLE {full_table}"), &[])
            .await?;

        Ok(())
    }

    /// Explicit table mode should fail when table is missing or has no visible columns.
    #[tokio::test]
    async fn sync_table_fails_when_explicit_table_has_no_columns() -> Result<()> {
        let db_url = match std::env::var("RUSTREAM_IT_DB_URL") {
            Ok(v) => v,
            Err(_) => return Ok(()), // Optional integration-style test.
        };

        let (client, connection) = tokio_postgres::connect(&db_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let table = TableConfig {
            name: format!("rustream_it_missing_table_{suffix}"),
            schema: Some("public".to_string()),
            columns: None,
            incremental_column: None,
            incremental_tiebreaker_column: None,
            incremental_column_is_unique: false,
            partition_by: None,
        };

        let tmp = tempfile::tempdir()?;
        let out_dir = tmp.path().join("out");
        let state_dir = tmp.path().join("state");
        std::fs::create_dir_all(&out_dir)?;
        std::fs::create_dir_all(&state_dir)?;

        let state = StateStore::open(state_dir.to_str().unwrap())?;
        let config = Config {
            postgres: PostgresConfig {
                host: "localhost".to_string(),
                port: 5432,
                database: "ignored_for_sync_table_test".to_string(),
                user: "ignored".to_string(),
                password: None,
            },
            output: Some(OutputConfig::Local {
                path: out_dir.to_string_lossy().to_string(),
            }),
            tables: Some(vec![table.clone()]),
            exclude: vec![],
            schema: "public".to_string(),
            batch_size: 2,
            state_dir: Some(state_dir.to_string_lossy().to_string()),
            format: OutputFormat::Parquet,
            catalog: None,
            warehouse: None,
            ingest: None,
        };

        let err = sync_table(&client, &config, &table, &state, true, None)
            .await
            .expect_err("explicit missing table should fail");
        assert!(err.to_string().contains("has no visible columns"));

        Ok(())
    }

    /// Verifies unique-watermark mode works without a tiebreaker and persists progress.
    #[tokio::test]
    async fn sync_table_supports_unique_watermark_without_tiebreaker() -> Result<()> {
        let db_url = match std::env::var("RUSTREAM_IT_DB_URL") {
            Ok(v) => v,
            Err(_) => return Ok(()), // Optional integration-style test.
        };

        let (client, connection) = tokio_postgres::connect(&db_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let table_name = format!("rustream_it_sync_unique_wm_{suffix}");
        let full_table = format!("public.{table_name}");

        client
            .execute(
                &format!(
                    "CREATE TABLE {full_table} (
                        id INTEGER PRIMARY KEY,
                        payload TEXT
                    )"
                ),
                &[],
            )
            .await?;

        client
            .execute(
                &format!(
                    "INSERT INTO {full_table} (id, payload) VALUES
                    (1, 'a'),
                    (2, 'b'),
                    (3, 'c'),
                    (4, 'd')"
                ),
                &[],
            )
            .await?;

        let tmp = tempfile::tempdir()?;
        let out_dir = tmp.path().join("out");
        let state_dir = tmp.path().join("state");
        std::fs::create_dir_all(&out_dir)?;
        std::fs::create_dir_all(&state_dir)?;

        let table = TableConfig {
            name: table_name.clone(),
            schema: Some("public".to_string()),
            columns: None,
            incremental_column: Some("id".to_string()),
            incremental_tiebreaker_column: None,
            incremental_column_is_unique: true,
            partition_by: None,
        };

        let state = StateStore::open(state_dir.to_str().unwrap())?;
        let config = Config {
            postgres: PostgresConfig {
                host: "localhost".to_string(),
                port: 5432,
                database: "ignored_for_sync_table_test".to_string(),
                user: "ignored".to_string(),
                password: None,
            },
            output: Some(OutputConfig::Local {
                path: out_dir.to_string_lossy().to_string(),
            }),
            tables: None,
            exclude: vec![],
            schema: "public".to_string(),
            batch_size: 2,
            state_dir: Some(state_dir.to_string_lossy().to_string()),
            format: OutputFormat::Parquet,
            catalog: None,
            warehouse: None,
            ingest: None,
        };

        sync_table(&client, &config, &table, &state, false, None).await?;
        let progress = state.get_progress(&full_table)?.unwrap();
        assert_eq!(progress.0, "4");
        assert_eq!(progress.1, None);

        client
            .execute(&format!("DROP TABLE {full_table}"), &[])
            .await?;

        Ok(())
    }
}
