use std::collections::VecDeque;
use std::sync::Arc;
use arrow::array::{ArrayRef, Float64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use crate::compute::ComputeStage;

pub struct RollingMean {
    column: String,
    window: usize,
    history: VecDeque<f64>,
    sum: f64,
}

impl RollingMean {
    pub fn new(column: String, window:usize) -> Self {
        Self {
            column,
            window,
            history: VecDeque::with_capacity(window),
            sum: 0.0,
        }
    }
}

impl ComputeStage for RollingMean {
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
            self.sum += val;
            if self.history.len() > self.window {
                self.sum -= self.history.pop_front().unwrap();
            }
            if self.history.len() == self.window {
                output.push(self.sum / self.window as f64);
            } else {
                output.push(f64::NAN);
            }
        }
        
        // build new schema with new column for rol mean
        let out_col_name = format!("{}_rolling_mean_{}", self.column, self.window);
        let new_col : ArrayRef = Arc::new(Float64Array::from(output));

        let mut fields: Vec<Field> = batch.schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        
        fields.push(Field::new(&out_col_name, DataType::Float64, true));

        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
        columns.push(new_col);
        
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build batch")
    }
}