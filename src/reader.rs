use anyhow::{Context, Result};
use arrow::array::*;
use arrow::datatypes::{DataType, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{NaiveDate, NaiveDateTime};
use std::sync::Arc;
use tokio_postgres::types::Type;
use tokio_postgres::{Client, Row};

use crate::config::TableConfig;
use crate::schema::ColumnInfo;

/// Build the SELECT query for a table, optionally filtering by watermark.
pub fn build_query(
    table: &TableConfig,
    columns: &[ColumnInfo],
    watermark_col: Option<&str>,
    watermark_val: Option<&str>,
    batch_size: usize,
) -> String {
    let cols = columns
        .iter()
        .map(|c| format!("\"{}\"", c.name))
        .collect::<Vec<_>>()
        .join(", ");

    let full_name = table.full_name();
    let mut query = format!("SELECT {cols} FROM {full_name}");

    if let (Some(col), Some(val)) = (watermark_col, watermark_val) {
        query.push_str(&format!(" WHERE \"{col}\" > '{val}'"));
    }

    if let Some(col) = watermark_col {
        query.push_str(&format!(" ORDER BY \"{col}\" ASC"));
    }

    query.push_str(&format!(" LIMIT {batch_size}"));
    query
}

/// Read rows from Postgres and convert to an Arrow RecordBatch.
pub async fn read_batch(
    client: &Client,
    table: &TableConfig,
    columns: &[ColumnInfo],
    schema: &Arc<Schema>,
    watermark_col: Option<&str>,
    watermark_val: Option<&str>,
    batch_size: usize,
) -> Result<Option<RecordBatch>> {
    let query = build_query(table, columns, watermark_col, watermark_val, batch_size);

    tracing::debug!(%query, "executing query");

    let rows = client
        .query(&query as &str, &[])
        .await
        .with_context(|| format!("querying table {}", table.full_name()))?;

    if rows.is_empty() {
        return Ok(None);
    }

    let arrays = columns
        .iter()
        .enumerate()
        .map(|(i, col)| rows_to_array(&rows, i, &col.arrow_type))
        .collect::<Result<Vec<_>>>()?;

    let batch = RecordBatch::try_new(schema.clone(), arrays)
        .context("building RecordBatch from PG rows")?;

    Ok(Some(batch))
}

/// Convert a column from PG rows into an Arrow Array.
fn rows_to_array(rows: &[Row], col_idx: usize, arrow_type: &DataType) -> Result<Arc<dyn Array>> {
    match arrow_type {
        DataType::Boolean => {
            let arr: BooleanArray = rows
                .iter()
                .map(|r| r.get::<_, Option<bool>>(col_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        DataType::Int16 => {
            let arr: Int16Array = rows
                .iter()
                .map(|r| r.get::<_, Option<i16>>(col_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        DataType::Int32 => {
            let arr: Int32Array = rows
                .iter()
                .map(|r| r.get::<_, Option<i32>>(col_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        DataType::Int64 => {
            let arr: Int64Array = rows
                .iter()
                .map(|r| r.get::<_, Option<i64>>(col_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        DataType::Float32 => {
            let arr: Float32Array = rows
                .iter()
                .map(|r| r.get::<_, Option<f32>>(col_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        DataType::Float64 => {
            let arr: Float64Array = rows
                .iter()
                .map(|r| r.get::<_, Option<f64>>(col_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        DataType::Date32 => {
            let arr: Date32Array = rows
                .iter()
                .map(|r| {
                    r.get::<_, Option<NaiveDate>>(col_idx).map(|d| {
                        d.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                            .num_days() as i32
                    })
                })
                .collect();
            Ok(Arc::new(arr))
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let arr: TimestampMicrosecondArray = rows
                .iter()
                .map(|r| {
                    r.get::<_, Option<NaiveDateTime>>(col_idx)
                        .map(|dt| dt.and_utc().timestamp_micros())
                })
                .collect();
            Ok(Arc::new(arr))
        }
        DataType::Timestamp(TimeUnit::Microsecond, Some(_tz)) => {
            let arr: TimestampMicrosecondArray = rows
                .iter()
                .map(|r| {
                    r.get::<_, Option<chrono::DateTime<chrono::Utc>>>(col_idx)
                        .map(|dt| dt.timestamp_micros())
                })
                .collect();
            let arr = arr.with_timezone("UTC");
            Ok(Arc::new(arr))
        }
        DataType::Binary => {
            let arr: BinaryArray = rows
                .iter()
                .map(|r| r.get::<_, Option<Vec<u8>>>(col_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        // Utf8 is the catch-all: UUID, JSON, numeric, text, etc.
        _ => {
            let arr: StringArray = rows.iter().map(|r| get_as_string(r, col_idx)).collect();
            Ok(Arc::new(arr))
        }
    }
}

/// Try to get a PG column value as a String, handling various types.
fn get_as_string(row: &Row, idx: usize) -> Option<String> {
    let col_type = row.columns()[idx].type_();

    match *col_type {
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => row.get::<_, Option<String>>(idx),
        Type::UUID => row.get::<_, Option<uuid::Uuid>>(idx).map(|u| u.to_string()),
        Type::JSON | Type::JSONB => row
            .get::<_, Option<serde_json::Value>>(idx)
            .map(|v| v.to_string()),
        Type::NUMERIC => row.get::<_, Option<String>>(idx),
        Type::TIMESTAMP => row
            .get::<_, Option<NaiveDateTime>>(idx)
            .map(|dt| dt.to_string()),
        Type::TIMESTAMPTZ => row
            .get::<_, Option<chrono::DateTime<chrono::Utc>>>(idx)
            .map(|dt| dt.to_rfc3339()),
        Type::DATE => row.get::<_, Option<NaiveDate>>(idx).map(|d| d.to_string()),
        Type::INT4 => row.get::<_, Option<i32>>(idx).map(|v| v.to_string()),
        Type::INT8 => row.get::<_, Option<i64>>(idx).map(|v| v.to_string()),
        Type::INT2 => row.get::<_, Option<i16>>(idx).map(|v| v.to_string()),
        Type::FLOAT4 => row.get::<_, Option<f32>>(idx).map(|v| v.to_string()),
        Type::FLOAT8 => row.get::<_, Option<f64>>(idx).map(|v| v.to_string()),
        Type::BOOL => row.get::<_, Option<bool>>(idx).map(|v| v.to_string()),
        _ => {
            // Last resort: try as String
            row.try_get::<_, Option<String>>(idx).unwrap_or(None)
        }
    }
}
