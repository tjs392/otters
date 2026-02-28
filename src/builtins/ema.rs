use arrow::array::Float64Array;
use arrow::record_batch::RecordBatch;
use crate::compute::ComputeStage;
use crate::builtins::zscore::append_column;

pub struct Ema {
    column: String,

    // this is the lookback period
    span: usize,
    current: Option<f64>,
}

impl Ema {
    pub fn new(column: String, span: usize) -> Self {
        Self { 
            column, span, current: None }
    }
}

impl ComputeStage for Ema {
    fn process(&mut self, batch: RecordBatch) -> RecordBatch {
        let col_idx = batch.schema().index_of(&self.column)
            .expect("column not found");

        // we need to downcast from the generic arrow array to float 64 array
        // this is so we can call .values() to get the raw &[f64] slice
        let col = batch.column(col_idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("column is not f64");

        // alpha for smoothing
        // std func is 2 / (span + 1)
        // higher span means smaller alpha
        // ema 20 has alpha .095, ema 5 has alpha .333, etc.
        let alpha = 2.0 / (self.span as f64 + 1.0);
        let mut output = Vec::with_capacity(col.len());

        // col.values() returns a raw &[f64] with no boing or heap alloc
        // just a slice directly into the arrow buf
        // this is why arrow compute is so fast cause theres no copy
        for val in col.values() {
            let ema = match self.current {
                None => *val,
                // new = alpha * current + (1 - alpha) * previous
                Some(prev) => alpha * val + (1.0 - alpha) * prev,
            };
            self.current = Some(ema);
            output.push(ema);
        }

        append_column(batch, output, format!("{}_ema_{}", self.column, self.span))
    }
}