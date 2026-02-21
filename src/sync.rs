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

    for table in &tables {
        if let Err(e) = sync_table(&client, &config, table, &state, iceberg_catalog.as_ref()).await
        {
            tracing::error!(table = %table.full_name(), error = %e, "failed to sync table");
        }
    }

    tracing::info!("sync complete");
    Ok(())
}

async fn sync_table(
    client: &tokio_postgres::Client,
    config: &Config,
    table: &TableConfig,
    state: &StateStore,
    iceberg_catalog: Option<&Arc<dyn Catalog>>,
) -> Result<()> {
    let table_name = table.full_name();
    tracing::info!(table = %table_name, "starting sync");

    // Introspect schema
    let columns = schema::introspect_table(client, table).await?;
    if columns.is_empty() {
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
    use arrow::array::{Int32Array, Int64Array, StringArray, TimestampMicrosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

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
}
