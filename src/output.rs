use anyhow::{Context, Result};
use std::path::Path;

use crate::config::OutputConfig;

/// Write bytes to the configured output target.
pub async fn write_output(config: &OutputConfig, key: &str, data: Vec<u8>) -> Result<()> {
    match config {
        OutputConfig::Local { path } => write_local(path, key, data).await,
        OutputConfig::S3 {
            bucket,
            prefix,
            region,
            endpoint,
        } => {
            write_s3(
                bucket,
                prefix,
                region.as_deref(),
                endpoint.as_deref(),
                key,
                data,
            )
            .await
        }
    }
}

async fn write_local(base_path: &str, key: &str, data: Vec<u8>) -> Result<()> {
    let full_path = Path::new(base_path).join(key);

    if let Some(parent) = full_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("creating directory {}", parent.display()))?;
    }

    tokio::fs::write(&full_path, data)
        .await
        .with_context(|| format!("writing file {}", full_path.display()))?;

    tracing::info!(path = %full_path.display(), "wrote Parquet file");
    Ok(())
}

async fn write_s3(
    bucket: &str,
    prefix: &str,
    region: Option<&str>,
    endpoint: Option<&str>,
    key: &str,
    data: Vec<u8>,
) -> Result<()> {
    let mut config_loader = aws_config::from_env();
    if let Some(r) = region {
        config_loader = config_loader.region(aws_config::Region::new(r.to_string()));
    }
    let aws_config = config_loader.load().await;

    let endpoint_override = endpoint
        .map(ToOwned::to_owned)
        .or_else(|| std::env::var("RUSTREAM_S3_ENDPOINT").ok());

    let client = if let Some(endpoint_url) = endpoint_override {
        let s3_cfg = aws_sdk_s3::config::Builder::from(&aws_config)
            .endpoint_url(endpoint_url)
            .force_path_style(true)
            .build();
        aws_sdk_s3::Client::from_conf(s3_cfg)
    } else {
        aws_sdk_s3::Client::new(&aws_config)
    };

    let s3_key = format!("{}/{}", prefix.trim_end_matches('/'), key);

    client
        .put_object()
        .bucket(bucket)
        .key(&s3_key)
        .body(data.into())
        .content_type("application/octet-stream")
        .send()
        .await
        .with_context(|| format!("uploading to s3://{bucket}/{s3_key}"))?;

    tracing::info!(bucket, key = %s3_key, "uploaded Parquet file to S3");
    Ok(())
}
