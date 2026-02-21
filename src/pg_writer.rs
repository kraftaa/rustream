use anyhow::{bail, Context, Result};
use arrow::array::*;
use arrow::datatypes::{DataType, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use tokio_postgres::types::ToSql;
use tokio_postgres::Client;

use crate::config::WriteMode;
use crate::types::arrow_type_to_pg;

/// Check if a table exists in the given schema.
pub async fn table_exists(client: &Client, table: &str, schema: &str) -> Result<bool> {
    let row = client
        .query_one(
            "SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = $1 AND table_name = $2
            )",
            &[&schema, &table],
        )
        .await
        .context("checking if table exists")?;

    Ok(row.get(0))
}

/// Create a table from an Arrow schema if it doesn't exist.
pub async fn ensure_table(
    client: &Client,
    arrow_schema: &Schema,
    table: &str,
    schema: &str,
) -> Result<()> {
    if table_exists(client, table, schema).await? {
        return Ok(());
    }

    let ddl = create_table_ddl(arrow_schema, table, schema);
    tracing::info!(table, schema, "creating table");
    client
        .execute(&ddl, &[])
        .await
        .with_context(|| format!("creating table {schema}.{table}"))?;

    Ok(())
}

/// Truncate a table.
pub async fn truncate_table(client: &Client, table: &str, schema: &str) -> Result<()> {
    let sql = format!("TRUNCATE TABLE \"{schema}\".\"{table}\"");
    client
        .execute(&sql, &[])
        .await
        .with_context(|| format!("truncating table {schema}.{table}"))?;
    Ok(())
}

/// Generate a CREATE TABLE DDL from an Arrow schema.
pub fn create_table_ddl(arrow_schema: &Schema, table: &str, schema: &str) -> String {
    let columns: Vec<String> = arrow_schema
        .fields()
        .iter()
        .map(|field| {
            let pg_type = arrow_type_to_pg(field.data_type());
            let nullable = if field.is_nullable() { "" } else { " NOT NULL" };
            format!("    \"{}\" {}{}", field.name(), pg_type, nullable)
        })
        .collect();

    format!(
        "CREATE TABLE \"{}\".\"{}\" (\n{}\n)",
        schema,
        table,
        columns.join(",\n")
    )
}

/// Build a multi-row INSERT SQL statement.
pub fn build_insert_sql(table: &str, schema: &str, columns: &[&str], num_rows: usize) -> String {
    let col_list: String = columns
        .iter()
        .map(|c| format!("\"{}\"", c))
        .collect::<Vec<_>>()
        .join(", ");

    let num_cols = columns.len();
    let mut value_rows = Vec::with_capacity(num_rows);
    for row_idx in 0..num_rows {
        let placeholders: Vec<String> = (0..num_cols)
            .map(|col_idx| format!("${}", row_idx * num_cols + col_idx + 1))
            .collect();
        value_rows.push(format!("({})", placeholders.join(", ")));
    }

    format!(
        "INSERT INTO \"{}\".\"{}\" ({}) VALUES {}",
        schema,
        table,
        col_list,
        value_rows.join(", ")
    )
}

/// Build a multi-row UPSERT (INSERT ... ON CONFLICT DO UPDATE) SQL statement.
pub fn build_upsert_sql(
    table: &str,
    schema: &str,
    columns: &[&str],
    key_columns: &[String],
    num_rows: usize,
) -> String {
    let insert_sql = build_insert_sql(table, schema, columns, num_rows);

    let key_list: String = key_columns
        .iter()
        .map(|c| format!("\"{}\"", c))
        .collect::<Vec<_>>()
        .join(", ");

    let update_set: String = columns
        .iter()
        .filter(|c| !key_columns.iter().any(|k| k == *c))
        .map(|c| format!("\"{}\" = EXCLUDED.\"{}\"", c, c))
        .collect::<Vec<_>>()
        .join(", ");

    if update_set.is_empty() {
        // All columns are keys, nothing to update — just do nothing
        format!("{insert_sql} ON CONFLICT ({key_list}) DO NOTHING")
    } else {
        format!("{insert_sql} ON CONFLICT ({key_list}) DO UPDATE SET {update_set}")
    }
}

/// Write a RecordBatch to Postgres using batch INSERT or UPSERT.
pub async fn write_batch(
    client: &Client,
    batch: &RecordBatch,
    table: &str,
    schema: &str,
    write_mode: &WriteMode,
    key_columns: &[String],
    batch_size: usize,
) -> Result<u64> {
    let arrow_schema = batch.schema();
    let columns: Vec<&str> = arrow_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    let num_cols = columns.len();
    let total_rows = batch.num_rows();
    let mut rows_written = 0u64;

    // Process in chunks of batch_size
    let mut offset = 0;
    while offset < total_rows {
        let chunk_size = (total_rows - offset).min(batch_size);
        let chunk = batch.slice(offset, chunk_size);

        let sql = match write_mode {
            WriteMode::Insert | WriteMode::TruncateInsert => {
                build_insert_sql(table, schema, &columns, chunk_size)
            }
            WriteMode::Upsert => build_upsert_sql(table, schema, &columns, key_columns, chunk_size),
        };

        // Extract all values into a flat Vec
        let params = extract_params(&chunk)?;

        // Build the references for tokio-postgres
        let param_refs: Vec<&(dyn ToSql + Sync)> = params
            .iter()
            .map(|p| p.as_ref() as &(dyn ToSql + Sync))
            .collect();

        // Validate param count
        let expected = chunk_size * num_cols;
        if param_refs.len() != expected {
            bail!(
                "parameter count mismatch: expected {expected}, got {}",
                param_refs.len()
            );
        }

        client
            .execute(&sql, &param_refs)
            .await
            .with_context(|| format!("executing batch insert into {schema}.{table}"))?;

        rows_written += chunk_size as u64;
        offset += chunk_size;
    }

    Ok(rows_written)
}

/// Extract all values from a RecordBatch as a flat Vec of boxed ToSql values.
/// Values are in row-major order: row0_col0, row0_col1, ..., row1_col0, ...
fn extract_params(batch: &RecordBatch) -> Result<Vec<Box<dyn ToSql + Sync>>> {
    let num_rows = batch.num_rows();
    let num_cols = batch.num_columns();
    let mut params: Vec<Box<dyn ToSql + Sync>> = Vec::with_capacity(num_rows * num_cols);

    for row_idx in 0..num_rows {
        for col_idx in 0..num_cols {
            let array = batch.column(col_idx);
            let value = extract_value(array, row_idx)?;
            params.push(value);
        }
    }

    Ok(params)
}

/// Extract a single value from an Arrow array at the given row index.
fn extract_value(array: &dyn Array, row_idx: usize) -> Result<Box<dyn ToSql + Sync>> {
    if array.is_null(row_idx) {
        return Ok(Box::new(None::<String>));
    }

    let dt = array.data_type();
    match dt {
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(Box::new(arr.value(row_idx)))
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            Ok(Box::new(arr.value(row_idx) as i16))
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            Ok(Box::new(arr.value(row_idx)))
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(Box::new(arr.value(row_idx)))
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(Box::new(arr.value(row_idx)))
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            Ok(Box::new(arr.value(row_idx) as i16))
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            Ok(Box::new(arr.value(row_idx) as i32))
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            Ok(Box::new(arr.value(row_idx) as i64))
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            Ok(Box::new(arr.value(row_idx) as i64))
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            Ok(Box::new(arr.value(row_idx)))
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            Ok(Box::new(arr.value(row_idx)))
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(Box::new(arr.value(row_idx).to_string()))
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            Ok(Box::new(arr.value(row_idx).to_string()))
        }
        DataType::Binary => {
            let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            Ok(Box::new(arr.value(row_idx).to_vec()))
        }
        DataType::LargeBinary => {
            let arr = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            Ok(Box::new(arr.value(row_idx).to_vec()))
        }
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            let days = arr.value(row_idx);
            let date = NaiveDate::from_num_days_from_ce_opt(days + 719_163)
                .context("invalid Date32 value")?;
            Ok(Box::new(date))
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            let micros = arr.value(row_idx);
            let secs = micros / 1_000_000;
            let nsecs = ((micros % 1_000_000) * 1000) as u32;
            let dt = chrono::DateTime::from_timestamp(secs, nsecs)
                .map(|dt| dt.naive_utc())
                .context("invalid timestamp value")?;
            Ok(Box::new(dt))
        }
        DataType::Timestamp(TimeUnit::Microsecond, Some(_tz)) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            let micros = arr.value(row_idx);
            let dt = chrono::DateTime::from_timestamp_micros(micros)
                .context("invalid timestamptz value")?;
            Ok(Box::new(dt))
        }
        DataType::Timestamp(TimeUnit::Millisecond, None) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            let millis = arr.value(row_idx);
            let secs = millis / 1000;
            let nsecs = ((millis % 1000) * 1_000_000) as u32;
            let dt = chrono::DateTime::from_timestamp(secs, nsecs)
                .map(|dt| dt.naive_utc())
                .context("invalid timestamp value")?;
            Ok(Box::new(dt))
        }
        DataType::Timestamp(TimeUnit::Millisecond, Some(_tz)) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            let millis = arr.value(row_idx);
            let dt = chrono::DateTime::from_timestamp_millis(millis)
                .context("invalid timestamptz value")?;
            Ok(Box::new(dt))
        }
        DataType::Timestamp(TimeUnit::Second, None) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap();
            let secs = arr.value(row_idx);
            let dt = chrono::DateTime::from_timestamp(secs, 0)
                .map(|dt| dt.naive_utc())
                .context("invalid timestamp value")?;
            Ok(Box::new(dt))
        }
        DataType::Timestamp(TimeUnit::Second, Some(_tz)) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap();
            let secs = arr.value(row_idx);
            let dt =
                chrono::DateTime::from_timestamp(secs, 0).context("invalid timestamptz value")?;
            Ok(Box::new(dt))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, None) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            let nanos = arr.value(row_idx);
            let secs = nanos / 1_000_000_000;
            let nsecs = (nanos % 1_000_000_000) as u32;
            let dt = chrono::DateTime::from_timestamp(secs, nsecs)
                .map(|dt| dt.naive_utc())
                .context("invalid timestamp value")?;
            Ok(Box::new(dt))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, Some(_tz)) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            let nanos = arr.value(row_idx);
            let secs = nanos / 1_000_000_000;
            let nsecs = (nanos % 1_000_000_000) as u32;
            let dt = chrono::DateTime::from_timestamp(secs, nsecs)
                .context("invalid timestamptz value")?;
            Ok(Box::new(dt))
        }
        // Fallback: convert to string
        _ => {
            let formatter = arrow::util::display::ArrayFormatter::try_new(
                array,
                &arrow::util::display::FormatOptions::default(),
            )
            .context("creating array formatter")?;
            Ok(Box::new(formatter.value(row_idx).to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_insert_sql() {
        let sql = build_insert_sql("users", "public", &["id", "name", "email"], 2);
        assert_eq!(
            sql,
            "INSERT INTO \"public\".\"users\" (\"id\", \"name\", \"email\") VALUES ($1, $2, $3), ($4, $5, $6)"
        );
    }

    #[test]
    fn test_build_insert_sql_single_row() {
        let sql = build_insert_sql("users", "public", &["id"], 1);
        assert_eq!(sql, "INSERT INTO \"public\".\"users\" (\"id\") VALUES ($1)");
    }

    #[test]
    fn test_build_upsert_sql() {
        let sql = build_upsert_sql(
            "users",
            "public",
            &["id", "name", "email"],
            &["id".to_string()],
            2,
        );
        assert_eq!(
            sql,
            "INSERT INTO \"public\".\"users\" (\"id\", \"name\", \"email\") \
             VALUES ($1, $2, $3), ($4, $5, $6) \
             ON CONFLICT (\"id\") DO UPDATE SET \"name\" = EXCLUDED.\"name\", \"email\" = EXCLUDED.\"email\""
        );
    }

    #[test]
    fn test_build_upsert_sql_composite_key() {
        let sql = build_upsert_sql(
            "order_items",
            "public",
            &["order_id", "item_id", "quantity"],
            &["order_id".to_string(), "item_id".to_string()],
            1,
        );
        assert!(sql.contains("ON CONFLICT (\"order_id\", \"item_id\")"));
        assert!(sql.contains("\"quantity\" = EXCLUDED.\"quantity\""));
    }

    #[test]
    fn test_build_upsert_all_keys() {
        let sql = build_upsert_sql("tags", "public", &["id"], &["id".to_string()], 1);
        assert!(sql.contains("ON CONFLICT (\"id\") DO NOTHING"));
    }

    #[test]
    fn test_create_table_ddl() {
        use arrow::datatypes::Field;

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, true),
        ]);

        let ddl = create_table_ddl(&schema, "users", "public");
        assert!(ddl.contains("\"id\" BIGINT NOT NULL"));
        assert!(ddl.contains("\"name\" TEXT"));
        assert!(ddl.contains("\"active\" BOOLEAN"));
        assert!(ddl.starts_with("CREATE TABLE \"public\".\"users\""));
    }
}
