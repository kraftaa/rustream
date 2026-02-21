use anyhow::{Context, Result};

use crate::config::{Config, IngestConfig, IngestTableConfig, WriteMode};
use crate::input;
use crate::pg_writer;
use crate::state::StateStore;
use crate::sync;

/// Resolve which target table a file should be ingested into.
/// First checks explicit table mappings, then infers from the file path.
fn resolve_target_table(file_key: &str, ingest: &IngestConfig) -> Option<String> {
    // Check explicit table mappings
    for table_cfg in &ingest.tables {
        if let Ok(pattern) = glob::Pattern::new(&table_cfg.file_pattern) {
            if pattern.matches(file_key) {
                return Some(table_cfg.target_table.clone());
            }
        }
    }

    // Infer from directory name (parent directory = table name)
    let path = std::path::Path::new(file_key);
    if let Some(parent) = path.parent() {
        let dir_name = parent
            .components()
            .next()
            .map(|c| c.as_os_str().to_string_lossy().to_string());
        if let Some(name) = dir_name {
            if !name.is_empty() {
                return Some(name);
            }
        }
    }

    // Last resort: use filename without extension
    path.file_stem().map(|s| s.to_string_lossy().to_string())
}

/// Find the IngestTableConfig for a given file, if any.
fn find_table_config<'a>(
    file_key: &str,
    ingest: &'a IngestConfig,
) -> Option<&'a IngestTableConfig> {
    ingest.tables.iter().find(|t| {
        glob::Pattern::new(&t.file_pattern)
            .map(|p| p.matches(file_key))
            .unwrap_or(false)
    })
}

/// Dry run: discover files and show what would be ingested.
pub async fn dry_run(config: Config) -> Result<()> {
    let ingest = config
        .ingest
        .as_ref()
        .context("'ingest' section required in config")?;

    let files = input::discover_files(&ingest.input).await?;

    if files.is_empty() {
        println!("No files found matching the configured pattern.");
        return Ok(());
    }

    println!("Would ingest {} files:\n", files.len());

    for file_key in &files {
        let target =
            resolve_target_table(file_key, ingest).unwrap_or_else(|| "(unknown)".to_string());
        let table_cfg = find_table_config(file_key, ingest);
        let create = table_cfg.is_some_and(|t| t.create_if_missing);

        println!(
            "  {} → {}{}",
            file_key,
            target,
            if create { " (create if missing)" } else { "" }
        );
    }

    println!("\nWrite mode: {:?}", ingest.write_mode);
    println!("File format: {:?}", ingest.file_format);
    println!("Batch size: {}", ingest.batch_size);

    Ok(())
}

/// Run the ingest process: read files → write to Postgres.
pub async fn run(config: Config) -> Result<()> {
    let ingest = config
        .ingest
        .clone()
        .context("'ingest' section required in config")?;

    let client = sync::connect(&config).await?;

    let state_dir = config
        .state_dir
        .clone()
        .unwrap_or_else(|| ".rustream_state".to_string());
    let state = StateStore::open(&state_dir)?;

    let files = input::discover_files(&ingest.input).await?;

    if files.is_empty() {
        tracing::info!("no files found matching pattern");
        return Ok(());
    }

    tracing::info!(files = files.len(), "discovered files for ingestion");

    let mut total_files = 0u64;
    let mut total_rows = 0u64;

    for file_key in &files {
        // Skip already-ingested files
        if state.is_file_ingested(file_key)? {
            tracing::debug!(file = %file_key, "skipping already-ingested file");
            continue;
        }

        let target_table = match resolve_target_table(file_key, &ingest) {
            Some(t) => t,
            None => {
                tracing::warn!(file = %file_key, "could not determine target table, skipping");
                continue;
            }
        };

        let table_cfg = find_table_config(file_key, &ingest);

        tracing::info!(file = %file_key, table = %target_table, "ingesting file");

        // Read the file
        let (schema, batches) = input::read_file(
            &ingest.input,
            file_key,
            &ingest.file_format,
            ingest.batch_size,
        )
        .await
        .with_context(|| format!("reading file {file_key}"))?;

        if batches.is_empty() {
            tracing::debug!(file = %file_key, "file is empty, skipping");
            continue;
        }

        // Ensure table exists if configured
        let create_if_missing = table_cfg.is_some_and(|t| t.create_if_missing);
        if create_if_missing {
            pg_writer::ensure_table(&client, &schema, &target_table, &ingest.target_schema).await?;
        }

        // Truncate if write mode is truncate_insert (only once per table per run)
        if ingest.write_mode == WriteMode::TruncateInsert {
            pg_writer::truncate_table(&client, &target_table, &ingest.target_schema).await?;
        }

        // Write batches
        let key_columns = table_cfg.map(|t| t.key_columns.clone()).unwrap_or_default();

        let mut file_rows = 0u64;
        for batch in &batches {
            let rows = pg_writer::write_batch(
                &client,
                batch,
                &target_table,
                &ingest.target_schema,
                &ingest.write_mode,
                &key_columns,
                ingest.batch_size,
            )
            .await
            .with_context(|| format!("writing batch to {target_table}"))?;
            file_rows += rows;
        }

        // Mark file as ingested
        state.mark_file_ingested(file_key, &target_table, file_rows)?;

        tracing::info!(
            file = %file_key,
            table = %target_table,
            rows = file_rows,
            "ingested file"
        );

        total_files += 1;
        total_rows += file_rows;
    }

    tracing::info!(files = total_files, rows = total_rows, "ingest complete");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{FileFormat, InputConfig};

    fn make_ingest_config(tables: Vec<IngestTableConfig>) -> IngestConfig {
        IngestConfig {
            input: InputConfig::Local {
                path: "./data".to_string(),
                pattern: "**/*.parquet".to_string(),
            },
            file_format: FileFormat::Parquet,
            write_mode: WriteMode::Insert,
            batch_size: 5000,
            tables,
            target_schema: "public".to_string(),
        }
    }

    #[test]
    fn test_resolve_target_table_explicit_mapping() {
        let ingest = make_ingest_config(vec![IngestTableConfig {
            file_pattern: "users/*.parquet".to_string(),
            target_table: "users".to_string(),
            key_columns: vec![],
            create_if_missing: false,
        }]);

        assert_eq!(
            resolve_target_table("users/part-0.parquet", &ingest),
            Some("users".to_string())
        );
    }

    #[test]
    fn test_resolve_target_table_infer_from_directory() {
        let ingest = make_ingest_config(vec![]);

        assert_eq!(
            resolve_target_table("orders/2024/data.parquet", &ingest),
            Some("orders".to_string())
        );
    }

    #[test]
    fn test_resolve_target_table_infer_from_filename() {
        let ingest = make_ingest_config(vec![]);

        assert_eq!(
            resolve_target_table("products.parquet", &ingest),
            Some("products".to_string())
        );
    }

    #[test]
    fn test_resolve_target_table_explicit_takes_priority() {
        let ingest = make_ingest_config(vec![IngestTableConfig {
            file_pattern: "raw/*.parquet".to_string(),
            target_table: "customers".to_string(),
            key_columns: vec![],
            create_if_missing: false,
        }]);

        assert_eq!(
            resolve_target_table("raw/data.parquet", &ingest),
            Some("customers".to_string())
        );
    }

    #[test]
    fn test_find_table_config() {
        let ingest = make_ingest_config(vec![
            IngestTableConfig {
                file_pattern: "users/*.parquet".to_string(),
                target_table: "users".to_string(),
                key_columns: vec!["id".to_string()],
                create_if_missing: true,
            },
            IngestTableConfig {
                file_pattern: "orders/**/*.parquet".to_string(),
                target_table: "orders".to_string(),
                key_columns: vec!["id".to_string()],
                create_if_missing: false,
            },
        ]);

        let cfg = find_table_config("users/part-0.parquet", &ingest);
        assert!(cfg.is_some());
        assert_eq!(cfg.unwrap().target_table, "users");

        let cfg = find_table_config("unknown/file.parquet", &ingest);
        assert!(cfg.is_none());
    }
}
