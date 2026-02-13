use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use iceberg::arrow::arrow_schema_to_schema;
use iceberg::spec::DataFileFormat;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use parquet::file::properties::WriterProperties;
use std::sync::Arc;

/// Write RecordBatches to an Iceberg table.
///
/// - Loads the table if it exists, otherwise creates it from the Arrow schema.
/// - Writes each batch through the Iceberg writer pipeline.
/// - Commits all data files in a single FastAppend transaction.
pub async fn write_iceberg(
    catalog: &Arc<dyn Catalog>,
    table_name: &str,
    batches: &[RecordBatch],
) -> Result<()> {
    if batches.is_empty() {
        return Ok(());
    }

    let arrow_schema = batches[0].schema();
    let namespace = NamespaceIdent::new("default".to_string());
    let table_ident = TableIdent::new(namespace.clone(), table_name.to_string());

    // Load or create the table
    let table = match catalog.table_exists(&table_ident).await {
        Ok(true) => catalog
            .load_table(&table_ident)
            .await
            .with_context(|| format!("loading Iceberg table {table_name}"))?,
        _ => {
            // Ensure namespace exists
            if catalog
                .list_namespaces(None)
                .await
                .map_or(true, |ns| !ns.iter().any(|n| n == &namespace))
            {
                let _ = catalog
                    .create_namespace(&namespace, Default::default())
                    .await;
            }

            let iceberg_schema = arrow_schema_to_schema(&arrow_schema)
                .context("converting Arrow schema to Iceberg schema")?;

            let creation = TableCreation::builder()
                .name(table_name.to_string())
                .schema(iceberg_schema)
                .build();

            catalog
                .create_table(&namespace, creation)
                .await
                .with_context(|| format!("creating Iceberg table {table_name}"))?
        }
    };

    tracing::info!(table = %table_name, "writing to Iceberg table");

    // Build the writer pipeline:
    //   ParquetWriterBuilder → RollingFileWriterBuilder → DataFileWriterBuilder
    let file_io = table.file_io().clone();
    let location_gen =
        DefaultLocationGenerator::new(table.metadata().clone()).context("location generator")?;
    let file_name_gen =
        DefaultFileNameGenerator::new("data".to_string(), None, DataFileFormat::Parquet);

    let props = WriterProperties::builder().build();
    let iceberg_schema = table.metadata().current_schema().clone();
    let parquet_builder = ParquetWriterBuilder::new(props, iceberg_schema);

    let rolling_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_builder,
        file_io,
        location_gen,
        file_name_gen,
    );

    let data_file_builder = DataFileWriterBuilder::new(rolling_builder);
    let mut writer = data_file_builder
        .build(None)
        .await
        .context("building Iceberg data file writer")?;

    // Write each batch
    for batch in batches {
        writer
            .write(batch.clone())
            .await
            .context("writing RecordBatch to Iceberg")?;
    }

    // Close writer to get data files
    let data_files = writer.close().await.context("closing Iceberg writer")?;

    if data_files.is_empty() {
        tracing::warn!(table = %table_name, "no data files produced");
        return Ok(());
    }

    tracing::info!(
        table = %table_name,
        files = data_files.len(),
        "committing data files"
    );

    // Commit via FastAppend transaction
    let tx = Transaction::new(&table);
    let action = tx.fast_append().add_data_files(data_files);
    let tx = action
        .apply(tx)
        .context("applying fast append to transaction")?;
    tx.commit(catalog.as_ref())
        .await
        .context("committing Iceberg transaction")?;

    tracing::info!(table = %table_name, "Iceberg commit complete");
    Ok(())
}
