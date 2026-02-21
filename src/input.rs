use anyhow::{Context, Result};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use crate::config::{FileFormat, InputConfig};

/// Discover files matching the configured pattern from local filesystem or S3.
pub async fn discover_files(input: &InputConfig) -> Result<Vec<String>> {
    match input {
        InputConfig::Local { path, pattern } => discover_local(path, pattern),
        InputConfig::S3 {
            bucket,
            prefix,
            region,
            pattern,
        } => discover_s3(bucket, prefix.as_deref(), region.as_deref(), pattern).await,
    }
}

/// Read a file into Arrow RecordBatches.
pub async fn read_file(
    input: &InputConfig,
    file_key: &str,
    format: &FileFormat,
    batch_size: usize,
) -> Result<(Arc<Schema>, Vec<RecordBatch>)> {
    match input {
        InputConfig::Local { path, .. } => {
            let full_path = std::path::Path::new(path).join(file_key);
            let full_path_str = full_path.to_str().context("invalid file path")?;
            match format {
                FileFormat::Parquet => read_parquet_file(full_path_str, batch_size),
                FileFormat::Csv => read_csv_file(full_path_str, batch_size),
            }
        }
        InputConfig::S3 {
            bucket,
            prefix,
            region,
            ..
        } => {
            let s3_key = match prefix {
                Some(p) => format!("{}/{}", p.trim_end_matches('/'), file_key),
                None => file_key.to_string(),
            };
            let data = read_s3_object(bucket, &s3_key, region.as_deref()).await?;
            match format {
                FileFormat::Parquet => read_parquet_bytes(&data, batch_size),
                FileFormat::Csv => read_csv_bytes(&data, batch_size),
            }
        }
    }
}

// --- Local file discovery ---

fn discover_local(base_path: &str, pattern: &str) -> Result<Vec<String>> {
    let full_pattern = format!(
        "{}/{}",
        base_path.trim_end_matches('/'),
        pattern.trim_start_matches('/')
    );

    let mut files: Vec<String> = Vec::new();
    for entry in glob::glob(&full_pattern)
        .with_context(|| format!("invalid glob pattern: {full_pattern}"))?
    {
        let path = entry.context("reading glob entry")?;
        if path.is_file() {
            // Store relative path from base_path
            let relative = path
                .strip_prefix(base_path)
                .unwrap_or(&path)
                .to_string_lossy()
                .to_string();
            files.push(relative);
        }
    }

    files.sort();
    Ok(files)
}

// --- S3 file discovery ---

async fn discover_s3(
    bucket: &str,
    prefix: Option<&str>,
    region: Option<&str>,
    pattern: &str,
) -> Result<Vec<String>> {
    let mut config_loader = aws_config::from_env();
    if let Some(r) = region {
        config_loader = config_loader.region(aws_config::Region::new(r.to_string()));
    }
    let aws_config = config_loader.load().await;
    let client = aws_sdk_s3::Client::new(&aws_config);

    let prefix_str = prefix.unwrap_or("");
    let mut files = Vec::new();
    let mut continuation_token: Option<String> = None;

    let glob_pattern =
        glob::Pattern::new(pattern).with_context(|| format!("invalid glob pattern: {pattern}"))?;

    loop {
        let mut req = client.list_objects_v2().bucket(bucket).prefix(prefix_str);
        if let Some(token) = &continuation_token {
            req = req.continuation_token(token);
        }

        let resp = req
            .send()
            .await
            .with_context(|| format!("listing objects in s3://{bucket}/{prefix_str}"))?;

        for obj in resp.contents() {
            if let Some(key) = obj.key() {
                // Strip prefix to get relative key
                let relative = if !prefix_str.is_empty() {
                    key.strip_prefix(prefix_str)
                        .unwrap_or(key)
                        .trim_start_matches('/')
                } else {
                    key
                };

                if glob_pattern.matches(relative) {
                    files.push(relative.to_string());
                }
            }
        }

        if resp.is_truncated() == Some(true) {
            continuation_token = resp.next_continuation_token().map(|s| s.to_string());
        } else {
            break;
        }
    }

    files.sort();
    Ok(files)
}

async fn read_s3_object(bucket: &str, key: &str, region: Option<&str>) -> Result<Vec<u8>> {
    let mut config_loader = aws_config::from_env();
    if let Some(r) = region {
        config_loader = config_loader.region(aws_config::Region::new(r.to_string()));
    }
    let aws_config = config_loader.load().await;
    let client = aws_sdk_s3::Client::new(&aws_config);

    let resp = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .with_context(|| format!("reading s3://{bucket}/{key}"))?;

    let data = resp
        .body
        .collect()
        .await
        .with_context(|| format!("reading body of s3://{bucket}/{key}"))?;

    Ok(data.into_bytes().to_vec())
}

// --- Parquet reading ---

fn read_parquet_file(path: &str, batch_size: usize) -> Result<(Arc<Schema>, Vec<RecordBatch>)> {
    let file = std::fs::File::open(path).with_context(|| format!("opening {path}"))?;
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("reading Parquet metadata from {path}"))?
        .with_batch_size(batch_size);

    let schema = Arc::new(builder.schema().as_ref().clone());
    let reader = builder
        .build()
        .with_context(|| format!("building Parquet reader for {path}"))?;

    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch.with_context(|| format!("reading batch from {path}"))?);
    }

    Ok((schema, batches))
}

fn read_parquet_bytes(data: &[u8], batch_size: usize) -> Result<(Arc<Schema>, Vec<RecordBatch>)> {
    let cursor = bytes::Bytes::from(data.to_vec());
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(cursor)
        .context("reading Parquet metadata from bytes")?
        .with_batch_size(batch_size);

    let schema = Arc::new(builder.schema().as_ref().clone());
    let reader = builder
        .build()
        .context("building Parquet reader from bytes")?;

    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch.context("reading batch from Parquet bytes")?);
    }

    Ok((schema, batches))
}

// --- CSV reading ---

fn read_csv_file(path: &str, batch_size: usize) -> Result<(Arc<Schema>, Vec<RecordBatch>)> {
    // First pass: infer schema
    let mut file = std::fs::File::open(path).with_context(|| format!("opening {path}"))?;
    let format = arrow_csv::reader::Format::default().with_header(true);
    let (inferred_schema, _) = format
        .infer_schema(&mut file, None)
        .with_context(|| format!("inferring CSV schema from {path}"))?;
    let schema = Arc::new(inferred_schema);

    // Second pass: read data
    let file = std::fs::File::open(path).with_context(|| format!("reopening {path}"))?;
    let reader = arrow_csv::ReaderBuilder::new(schema.clone())
        .with_batch_size(batch_size)
        .with_header(true)
        .build(file)
        .with_context(|| format!("building CSV reader for {path}"))?;

    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch.with_context(|| format!("reading batch from {path}"))?);
    }

    Ok((schema, batches))
}

fn read_csv_bytes(data: &[u8], batch_size: usize) -> Result<(Arc<Schema>, Vec<RecordBatch>)> {
    // First pass: infer schema
    let mut cursor = std::io::Cursor::new(data);
    let format = arrow_csv::reader::Format::default().with_header(true);
    let (inferred_schema, _) = format
        .infer_schema(&mut cursor, None)
        .context("inferring CSV schema from bytes")?;
    let schema = Arc::new(inferred_schema);

    // Second pass: read data
    let cursor = std::io::Cursor::new(data);
    let reader = arrow_csv::ReaderBuilder::new(schema.clone())
        .with_batch_size(batch_size)
        .with_header(true)
        .build(cursor)
        .context("building CSV reader from bytes")?;

    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch.context("reading batch from CSV bytes")?);
    }

    Ok((schema, batches))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field};

    #[test]
    fn test_discover_local_files() {
        let dir = tempfile::TempDir::new().unwrap();
        let base = dir.path();

        // Create subdirectories and files
        std::fs::create_dir_all(base.join("users")).unwrap();
        std::fs::create_dir_all(base.join("orders")).unwrap();
        std::fs::File::create(base.join("users/part-0.parquet")).unwrap();
        std::fs::File::create(base.join("users/part-1.parquet")).unwrap();
        std::fs::File::create(base.join("orders/data.parquet")).unwrap();
        std::fs::File::create(base.join("orders/data.csv")).unwrap();

        let files = discover_local(base.to_str().unwrap(), "**/*.parquet").unwrap();
        assert_eq!(files.len(), 3);
        assert!(files.contains(&"orders/data.parquet".to_string()));
        assert!(files.contains(&"users/part-0.parquet".to_string()));
        assert!(files.contains(&"users/part-1.parquet".to_string()));
    }

    #[test]
    fn test_discover_local_csv_pattern() {
        let dir = tempfile::TempDir::new().unwrap();
        let base = dir.path();

        std::fs::create_dir_all(base.join("data")).unwrap();
        std::fs::File::create(base.join("data/file.csv")).unwrap();
        std::fs::File::create(base.join("data/file.parquet")).unwrap();

        let files = discover_local(base.to_str().unwrap(), "**/*.csv").unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0], "data/file.csv");
    }

    #[test]
    fn test_read_parquet_roundtrip() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("test.parquet");

        // Write a test Parquet file
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["alice", "bob", "charlie"])),
            ],
        )
        .unwrap();

        let file = std::fs::File::create(&path).unwrap();
        let props = parquet::file::properties::WriterProperties::builder().build();
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Read it back
        let (read_schema, batches) = read_parquet_file(path.to_str().unwrap(), 1024).unwrap();
        assert_eq!(read_schema.fields().len(), 2);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);

        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(2), 3);
    }

    #[test]
    fn test_read_csv_roundtrip() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("test.csv");

        std::fs::write(&path, "id,name\n1,alice\n2,bob\n3,charlie\n").unwrap();

        let (schema, batches) = read_csv_file(path.to_str().unwrap(), 1024).unwrap();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[test]
    fn test_read_csv_bytes_roundtrip() {
        let data = b"x,y\n10,hello\n20,world\n";
        let (schema, batches) = read_csv_bytes(data, 1024).unwrap();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
    }
}
