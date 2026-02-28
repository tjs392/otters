use std::collections::VecDeque;
use std::sync::Arc;
use arrow::array::{ArrayRef, Float64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use crate::compute::ComputeStage;

pub struct ZScore {
    column: String,
    // window size
    lookback: usize,

    // rolling window of raw vals of size lookback
    history: VecDeque<f64>,
}

impl ZScore {
    pub fn new(column: String, lookback: usize) -> Self {
        Self {
            column,
            lookback,
            history: VecDeque::with_capacity(lookback),
        }
    }
}

impl ComputeStage for ZScore {
    // this is similar to the other builtins, downcast to get direct slices of arrow buffers, etc.
    fn process(&mut self, batch: RecordBatch) -> RecordBatch {
        let col_idx = batch.schema().index_of(&self.column)
            .expect("column not found");
        let col = batch.column(col_idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("column is not f64");

        let mut output = Vec::with_capacity(col.len());

        for val in col.values() {
            self.history.push_back(*val);
            if self.history.len() > self.lookback {
                self.history.pop_front();
            }

            if self.history.len() < self.lookback {
                output.push(f64::NAN);
                continue;
            }

            // this is O(lookback) per row right now
            // for small lookbacks that fine
            // but for large maybe 
            // TODO: implement large lookbck algorithm for o(1)
            let mean = self.history.iter().sum::<f64>() / self.lookback as f64;
            let variance = self.history.iter()
                .map(|x| (x - mean).powi(2))
                .sum::<f64>() / (self.lookback - 1) as f64;
            let std = variance.sqrt();

            output.push(if std == 0.0 { 0.0 } else { (val - mean) / std });
        }

        append_column(batch, output, format!("{}_zscore_{}", self.column, self.lookback))
    }
}

/// appends f64 column to exisitng arrow recordbatch
pub fn append_column(batch: RecordBatch, values: Vec<f64>, name: String) -> RecordBatch {
    let new_col: ArrayRef = Arc::new(Float64Array::from(values));
    let mut fields: Vec<Field> = batch.schema().fields().iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(Field::new(&name, DataType::Float64, true));

    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(new_col);

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .expect("failed to build output batch")
}