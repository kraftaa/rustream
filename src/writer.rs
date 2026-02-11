use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::io::Write;

/// Write RecordBatches to a Parquet file in a buffer.
///
/// Returns Ok(()) immediately if batches is empty (no file written).
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::sync::Arc;

    fn make_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("alice"), Some("bob"), None])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn write_empty_batches() {
        let mut buf = Vec::new();
        write_parquet(&mut buf, &[]).unwrap();
        assert!(buf.is_empty());
    }

    #[test]
    fn write_and_read_back() {
        let batch = make_batch();
        let mut buf = Vec::new();
        write_parquet(&mut buf, &[batch]).unwrap();

        // Parquet magic bytes: PAR1
        assert!(buf.len() > 4);
        assert_eq!(&buf[..4], b"PAR1");

        // Read back with parquet reader
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(buf))
            .unwrap()
            .build()
            .unwrap();

        let batches: Vec<RecordBatch> =
            reader.map(|r: Result<RecordBatch, _>| r.unwrap()).collect();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
        assert_eq!(batches[0].num_columns(), 2);

        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.values(), &[1, 2, 3]);

        let names = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "alice");
        assert_eq!(names.value(1), "bob");
        assert!(names.is_null(2));
    }

    #[test]
    fn write_multiple_batches() {
        let batch = make_batch();
        let mut buf = Vec::new();
        write_parquet(&mut buf, &[batch.clone(), batch]).unwrap();

        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(buf))
            .unwrap()
            .build()
            .unwrap();

        let total_rows: usize = reader
            .map(|r: Result<RecordBatch, _>| r.unwrap().num_rows())
            .sum();
        assert_eq!(total_rows, 6);
    }
}
