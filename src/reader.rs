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

pub struct ReadBatchOptions<'a> {
    pub watermark_col: Option<&'a str>,
    pub cursor_col: Option<&'a str>,
    pub watermark_val: Option<&'a str>,
    pub cursor_val: Option<&'a str>,
    pub batch_size: usize,
}

/// Build the SELECT query for a table, optionally filtering by watermark.
pub fn build_query(
    table: &TableConfig,
    columns: &[ColumnInfo],
    watermark_col: Option<&str>,
    cursor_col: Option<&str>,
    watermark_val: Option<&str>,
    cursor_val: Option<&str>,
    batch_size: usize,
) -> String {
    let cols = columns
        .iter()
        .map(|c| format!("\"{}\"", c.name))
        .collect::<Vec<_>>()
        .join(", ");

    let full_name = table.full_name();
    let mut query = format!("SELECT {cols} FROM {full_name}");

    if let (Some(wm_col), Some(_)) = (watermark_col, watermark_val) {
        if let (Some(cur_col), Some(_)) = (cursor_col, cursor_val) {
            query.push_str(&format!(
                " WHERE (\"{wm_col}\" > $1 OR (\"{wm_col}\" = $1 AND \"{cur_col}\" > $2))"
            ));
        } else {
            query.push_str(&format!(" WHERE \"{wm_col}\" > $1"));
        }
    }

    if let Some(wm_col) = watermark_col {
        if let Some(cur_col) = cursor_col {
            query.push_str(&format!(" ORDER BY \"{wm_col}\" ASC, \"{cur_col}\" ASC"));
        } else {
            query.push_str(&format!(" ORDER BY \"{wm_col}\" ASC"));
        }
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
    options: ReadBatchOptions<'_>,
) -> Result<Option<RecordBatch>> {
    let query = build_query(
        table,
        columns,
        options.watermark_col,
        options.cursor_col,
        options.watermark_val,
        options.cursor_val,
        options.batch_size,
    );

    tracing::debug!(%query, "executing query");

    let rows = match (options.watermark_val, options.cursor_val) {
        (Some(wm), Some(cur)) if options.cursor_col.is_some() => client
            .query(&query as &str, &[&wm, &cur])
            .await
            .with_context(|| format!("querying table {}", table.full_name()))?,
        (Some(wm), _) => client
            .query(&query as &str, &[&wm])
            .await
            .with_context(|| format!("querying table {}", table.full_name()))?,
        (None, _) => client
            .query(&query as &str, &[])
            .await
            .with_context(|| format!("querying table {}", table.full_name()))?,
    };

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema;
    use crate::sync::extract_watermark;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio_postgres::NoTls;

    fn make_table(name: &str) -> TableConfig {
        TableConfig {
            name: name.into(),
            schema: None,
            columns: None,
            incremental_column: None,
            incremental_tiebreaker_column: None,
            incremental_column_is_unique: false,
            partition_by: None,
        }
    }

    fn make_columns() -> Vec<ColumnInfo> {
        vec![
            ColumnInfo {
                name: "id".into(),
                pg_type: "integer".into(),
                arrow_type: DataType::Int32,
                is_nullable: false,
            },
            ColumnInfo {
                name: "name".into(),
                pg_type: "text".into(),
                arrow_type: DataType::Utf8,
                is_nullable: true,
            },
            ColumnInfo {
                name: "updated_at".into(),
                pg_type: "timestamptz".into(),
                arrow_type: DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                is_nullable: true,
            },
        ]
    }

    #[test]
    fn build_query_full_table() {
        let table = make_table("users");
        let cols = make_columns();
        let q = build_query(&table, &cols, None, None, None, None, 1000);
        assert_eq!(
            q,
            "SELECT \"id\", \"name\", \"updated_at\" FROM users LIMIT 1000"
        );
    }

    #[test]
    fn build_query_with_schema() {
        let table = TableConfig {
            name: "users".into(),
            schema: Some("analytics".into()),
            columns: None,
            incremental_column: None,
            incremental_tiebreaker_column: None,
            incremental_column_is_unique: false,
            partition_by: None,
        };
        let cols = make_columns();
        let q = build_query(&table, &cols, None, None, None, None, 500);
        assert_eq!(
            q,
            "SELECT \"id\", \"name\", \"updated_at\" FROM analytics.users LIMIT 500"
        );
    }

    #[test]
    fn build_query_with_watermark_no_value() {
        let table = make_table("users");
        let cols = make_columns();
        let q = build_query(&table, &cols, Some("updated_at"), None, None, None, 1000);
        assert_eq!(
            q,
            "SELECT \"id\", \"name\", \"updated_at\" FROM users ORDER BY \"updated_at\" ASC LIMIT 1000"
        );
    }

    #[test]
    fn build_query_with_watermark_value() {
        let table = make_table("users");
        let cols = make_columns();
        let q = build_query(
            &table,
            &cols,
            Some("updated_at"),
            None,
            Some("2026-01-01 00:00:00"),
            None,
            1000,
        );
        assert!(q.contains("WHERE \"updated_at\" > $1"));
        assert!(q.contains("ORDER BY \"updated_at\" ASC"));
        assert!(q.ends_with("LIMIT 1000"));
    }

    #[test]
    fn build_query_with_watermark_and_cursor() {
        let table = make_table("users");
        let cols = make_columns();
        let q = build_query(
            &table,
            &cols,
            Some("updated_at"),
            Some("id"),
            Some("2026-01-01 00:00:00"),
            Some("100"),
            1000,
        );
        assert!(q.contains("WHERE (\"updated_at\" > $1 OR (\"updated_at\" = $1 AND \"id\" > $2))"));
        assert!(q.contains("ORDER BY \"updated_at\" ASC, \"id\" ASC"));
        assert!(q.ends_with("LIMIT 1000"));
    }

    #[tokio::test]
    async fn read_batch_cursor_paging_handles_duplicate_watermarks() {
        let db_url = match std::env::var("RUSTREAM_IT_DB_URL") {
            Ok(v) => v,
            Err(_) => return, // Optional integration-style test; set env var to run.
        };

        let (client, connection) = tokio_postgres::connect(&db_url, NoTls).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let table_name = format!("rustream_it_reader_{suffix}");

        client
            .execute(
                &format!(
                    "CREATE TABLE public.{table_name} (
                        id INTEGER PRIMARY KEY,
                        updated_at TIMESTAMPTZ NOT NULL,
                        payload TEXT
                    )"
                ),
                &[],
            )
            .await
            .unwrap();

        client
            .execute(
                &format!(
                    "INSERT INTO public.{table_name} (id, updated_at, payload) VALUES
                    (1, '2026-01-01T00:00:00Z', 'a'),
                    (2, '2026-01-01T00:00:00Z', 'b'),
                    (3, '2026-01-01T00:00:00Z', 'c'),
                    (4, '2026-01-01T00:01:00Z', 'd')"
                ),
                &[],
            )
            .await
            .unwrap();

        let table = TableConfig {
            name: table_name.clone(),
            schema: Some("public".into()),
            columns: None,
            incremental_column: Some("updated_at".into()),
            incremental_tiebreaker_column: Some("id".into()),
            incremental_column_is_unique: false,
            partition_by: None,
        };
        let columns = schema::introspect_table(&client, &table).await.unwrap();
        let arrow_schema = schema::build_arrow_schema(&columns);

        let batch1 = read_batch(
            &client,
            &table,
            &columns,
            &arrow_schema,
            ReadBatchOptions {
                watermark_col: Some("updated_at"),
                cursor_col: Some("id"),
                watermark_val: None,
                cursor_val: None,
                batch_size: 2,
            },
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(batch1.num_rows(), 2);
        let wm1 = extract_watermark(&columns, &batch1, "updated_at").unwrap();
        let cur1 = extract_watermark(&columns, &batch1, "id").unwrap();

        let batch2 = read_batch(
            &client,
            &table,
            &columns,
            &arrow_schema,
            ReadBatchOptions {
                watermark_col: Some("updated_at"),
                cursor_col: Some("id"),
                watermark_val: Some(&wm1),
                cursor_val: Some(&cur1),
                batch_size: 2,
            },
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(batch2.num_rows(), 2);

        let id_col = columns.iter().position(|c| c.name == "id").unwrap();
        let ids = batch2
            .column(id_col)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 3);
        assert_eq!(ids.value(1), 4);

        let wm2 = extract_watermark(&columns, &batch2, "updated_at").unwrap();
        let cur2 = extract_watermark(&columns, &batch2, "id").unwrap();

        let batch3 = read_batch(
            &client,
            &table,
            &columns,
            &arrow_schema,
            ReadBatchOptions {
                watermark_col: Some("updated_at"),
                cursor_col: Some("id"),
                watermark_val: Some(&wm2),
                cursor_val: Some(&cur2),
                batch_size: 2,
            },
        )
        .await
        .unwrap();
        assert!(batch3.is_none());

        client
            .execute(&format!("DROP TABLE public.{table_name}"), &[])
            .await
            .unwrap();
    }
}
