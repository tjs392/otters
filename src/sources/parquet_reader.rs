use arrow::record_batch::RecordBatch;
use crossbeam_channel::Sender;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;

/// spawns background thread that reads a parquet file in batches
/// then sends each batch into the pipeline channel
/// returns a join handle so the caller can wait for it ot finish
pub fn spawn_parquet_source(
    path: String,
    sender: Sender<RecordBatch>,
    batch_size: usize,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let file = File::open(&path)
            .expect("failed to open parquet file");

        // reads parquet footer metadata (schema, row group offsets)
        // without loading the row data
        // with_batch_size controls how many rows come back per batch
        // which is the key to constant mem usage regardles of filesize
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .expect("failed to read parquet metadata")
            .with_batch_size(batch_size);

        // builds interator
        let reader = builder.build()
            .expect("failed to build parquet reader");

        // each it reads on batch from disk then sends it downstream
        // also handles backpressure
        for batch in reader {
            match batch {
                Ok(b) => { sender.send(b).ok(); }
                Err(e) => panic!("failed to read parquet batch: {}", e),
            }
        }
    })
}