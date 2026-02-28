use arrow::record_batch::RecordBatch;

pub trait ComputeStage: Send + Sync {
    fn process(&mut self, batch: RecordBatch) -> RecordBatch;
}