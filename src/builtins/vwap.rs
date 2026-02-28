use std::collections::VecDeque;
use arrow::array::Float64Array;
use arrow::record_batch::RecordBatch;
use crate::compute::ComputeStage;
use crate::builtins::zscore::append_column;

pub struct Vwap {
    price_col: String,
    volume_col: String,
    window: usize,
    // store (price * volume, volume) pairs
    history: VecDeque<(f64, f64)>,
}

impl Vwap {
    pub fn new(price_col: String, volume_col: String, window: usize) -> Self {
        Self {
            price_col,
            volume_col,
            window,
            history: VecDeque::with_capacity(window),
        }
    }
}

impl ComputeStage for Vwap {
    fn process(&mut self, batch: RecordBatch) -> RecordBatch {
        let price_idx = batch.schema().index_of(&self.price_col)
            .expect("price column not found");
        let volume_idx = batch.schema().index_of(&self.volume_col)
            .expect("volume column not found");

        let prices = batch.column(price_idx)
            .as_any().downcast_ref::<Float64Array>()
            .expect("price column is not f64");
        let volumes = batch.column(volume_idx)
            .as_any().downcast_ref::<Float64Array>()
            .expect("volume column is not f64");

        let mut output = Vec::with_capacity(batch.num_rows());

        for (price, volume) in prices.values().iter().zip(volumes.values().iter()) {
            self.history.push_back((price * volume, *volume));
            if self.history.len() > self.window {
                self.history.pop_front();
            }

            if self.history.len() < self.window {
                output.push(f64::NAN);
                continue;
            }

            let (pv_sum, v_sum) = self.history.iter()
                .fold((0.0, 0.0), |(pv, v), (pvi, vi)| (pv + pvi, v + vi));

            output.push(if v_sum == 0.0 { f64::NAN } else { pv_sum / v_sum });
        }

        append_column(batch, output, format!("vwap_{}", self.window))
    }
}