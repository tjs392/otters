use arrow::record_batch::RecordBatch;
use crossbeam_channel::Receiver;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;

/// spawns back ground thread that receives record batches from pipeline
/// then writes them to a parquet file.
/// no python dict conversion, data stays as arrow memory the whole time
pub fn spawn_parquet_sink(
    path: String,
    receiver: Receiver<RecordBatch>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        // lazily create writer since we don't know final schema until data is here
        let mut writer: Option<ArrowWriter<File>> = None;

        // receiver.iter() blocks
        // when the upstream channel closes the it ends and the loop exits
        for batch in receiver.iter() {
            if writer.is_none() {
                // create the writer lazily on first batch
                // so we know the schema (which may have new columns added by stages)
                let file = File::create(&path)
                    .expect("failed to create output parquet file");
                let props = WriterProperties::builder().build();
                writer = Some(
                    ArrowWriter::try_new(file, batch.schema(), Some(props))
                        .expect("failed to create parquet writer")
                );
            }
            writer.as_mut().unwrap()
                .write(&batch)
                .expect("failed to write batch to parquet");
        }

        if let Some(mut w) = writer {
            w.close().expect("failed to finalize parquet file");
        }
    })
}