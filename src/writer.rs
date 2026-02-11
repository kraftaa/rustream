use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::io::Write;

/// Write RecordBatches to a Parquet file in a buffer.
pub fn write_parquet<W: Write + Send>(writer: W, batches: &[RecordBatch]) -> Result<()> {
    if batches.is_empty() {
        return Ok(());
    }

    let schema = batches[0].schema();

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let mut writer =
        ArrowWriter::try_new(writer, schema, Some(props)).context("creating Parquet writer")?;

    for batch in batches {
        writer
            .write(batch)
            .context("writing RecordBatch to Parquet")?;
    }

    writer.close().context("closing Parquet writer")?;

    Ok(())
}
