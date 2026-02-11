use anyhow::{Context, Result};
use chrono::Utc;
use tokio_postgres::NoTls;

use crate::config::{Config, PartitionBy, TableConfig};
use crate::output;
use crate::reader;
use crate::schema::{self, ColumnInfo};
use crate::state::StateStore;
use crate::writer;

async fn connect(config: &Config) -> Result<tokio_postgres::Client> {
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
            "incremental"
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

    println!("\nOutput: {:?}", config.output);

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

    for table in &tables {
        if let Err(e) = sync_table(&client, &config, table, &state).await {
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
    let mut watermark_val = match watermark_col {
        Some(_) => state.get_watermark(&table_name)?,
        None => None,
    };

    let mut total_rows = 0u64;
    let mut batch_num = 0u32;

    loop {
        let batch = reader::read_batch(
            client,
            table,
            &columns,
            &arrow_schema,
            watermark_col,
            watermark_val.as_deref(),
            config.batch_size,
        )
        .await?;

        let batch = match batch {
            Some(b) if b.num_rows() > 0 => b,
            _ => break,
        };

        let num_rows = batch.num_rows();
        total_rows += num_rows as u64;

        // Update watermark from the last row
        if let Some(col) = watermark_col {
            if let Some(new_wm) = extract_watermark(&columns, &batch, col) {
                watermark_val = Some(new_wm);
            }
        }

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

        output::write_output(&config.output, &filename, buf).await?;

        batch_num += 1;

        // If we got fewer rows than batch_size, we're done
        if num_rows < config.batch_size {
            break;
        }
    }

    // Persist watermark
    if let (Some(_), Some(ref wm)) = (watermark_col, &watermark_val) {
        state.set_watermark(&table_name, wm)?;
        tracing::info!(table = %table_name, watermark = %wm, "updated watermark");
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
    use arrow::array::{Int64Array, StringArray, TimestampMicrosecondArray};
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
