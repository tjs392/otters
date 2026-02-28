use std::sync::Arc;
use crossbeam_channel::{Receiver, Sender};
use pyo3::prelude::*;
use arrow::record_batch::RecordBatch;
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use crate::compute::ComputeStage;
use crate::batcher::spawn_batcher;
use crate::builtins::rolling_mean::RollingMean;
use crate::builtins::zscore::ZScore;
use crate::builtins::ema::Ema;
use crate::builtins::vwap::Vwap;
use crate::sources::parquet_reader::spawn_parquet_source;
use crate::sinks::parquet_writer::spawn_parquet_sink;

/// what role a stage plays in the pipeline
/// 
/// source - produces item from a python iterator
/// 
/// sink   - consumes items and calls a python callback, no output channel
/// 
/// stage  - receives items and transforms via python callback, sends results
enum StageKind {
    Source(Py<PyAny>),
    ParquetSource(String),
    Sink(Py<PyAny>),
    ParquetSink(String),
    Stage(Box<dyn ComputeStage + Send + Sync>),
    PyTransform(Py<PyAny>),
}

/// internal config for a stage
/// 
/// just a thin wrapper around stagekind right now
/// TODO: add error handling policy and stage name for logging, etc.
struct StageConfig {
    kind: StageKind,
}

/// multi stage pipeline
/// 
/// stages are registerd in order with source(), rolling_mean()... etc. ... sink()
/// calling run() wires them together with bounded cahnnels, and spawn one thread for each stage
///     also blocks until the source is exhausted and all items have flowed through the sink
/// 
/// backpressure is automatic, if a stage falls behind its input channel
/// fills up and the upstream stage blocks on send
/// 
/// exposed to python via Py03 as otters.Pipeline
#[pyclass]
pub struct Pipeline {
    /// stages in pipeline order, drained during run()
    stages: Vec<StageConfig>,
    capacity: usize,
    batch_size: usize,
}

#[pymethods]
impl Pipeline {
    #[new]
    #[pyo3(signature = (capacity=1024, batch_size=2500))]
    pub fn new(capacity: usize, batch_size: usize) -> Pipeline {
        Pipeline { stages: vec![], capacity, batch_size }
    }

    fn source(&mut self, src: Py<PyAny>, py: Python<'_>) {
        if let Ok(s) = src.extract::<String>(py) {
            if s.ends_with(".parquet") {
                self.stages.push(StageConfig {
                    kind: StageKind::ParquetSource(s),
                });
                return;
            }
        }

        // fallback: python generator
        self.stages.push(StageConfig {
            kind: StageKind::Source(src),
        });
    }

    fn sink(&mut self, target: Py<PyAny>, py: Python<'_>) {
        if let Ok(s) = target.extract::<String>(py) {
            if s.ends_with(".parquet") {
                self.stages.push(StageConfig {
                    kind: StageKind::ParquetSink(s),
                });
                return;
            }
        }

        // fallback: python callable
        self.stages.push(StageConfig {
            kind: StageKind::Sink(target),
        });
    }

    ////stages

    fn rolling_mean(&mut self, column: String, window: usize) {
        self.stages.push(StageConfig {
            kind: StageKind::Stage(Box::new(RollingMean::new(column, window))),
        });
    }

    fn zscore(&mut self, column: String, lookback: usize) {
        self.stages.push(StageConfig {
            kind: StageKind::Stage(Box::new(ZScore::new(column, lookback))),
        });
    }

    fn ema(&mut self, column: String, span: usize) {
        self.stages.push(StageConfig {
            kind: StageKind::Stage(Box::new(Ema::new(column, span))),
        });
    }

    fn vwap(&mut self, price_col: String, volume_col: String, window: usize) {
        self.stages.push(StageConfig {
            kind: StageKind::Stage(Box::new(Vwap::new(price_col, volume_col, window))),
        });
    }

    fn py_transform(&mut self, callback: Py<PyAny>) {
        self.stages.push(StageConfig { kind: StageKind::PyTransform(callback) });
    }

    /// wires up channels between stages, spawns workers threads, and
    /// blocks until the pipeline finishes
    /// 
    /// must give py so can release GIL while waiting
    fn run(&mut self, py: Python<'_>) {
        let stages: Vec<StageConfig> = self.stages.drain(..).collect();
        let mut handles = Vec::new();
        let capacity = self.capacity;
        let batch_size = self.batch_size;

        let has_parquet_source = stages.iter()
            .any(|s| matches!(s.kind, StageKind::ParquetSource(_)));

        let rust_stage_count = stages.iter()
            .filter(|s| matches!(s.kind, StageKind::Stage(_) | StageKind::PyTransform(_)))
            .count();

        // batch channels: enough for all rust stages + 1 for source output
        let mut batch_senders: Vec<Option<Sender<RecordBatch>>> = Vec::new();
        let mut batch_receivers: Vec<Option<Receiver<RecordBatch>>> = Vec::new();
        for _ in 0..rust_stage_count + 1 {
            let (s, r) = crossbeam_channel::bounded::<RecordBatch>(capacity);
            batch_senders.push(Some(s));
            batch_receivers.push(Some(r));
        }

        // dict channel only needed for python generator source
        let dict_channel = if !has_parquet_source {
            let (tx, rx) = crossbeam_channel::bounded::<Py<PyAny>>(capacity);
            Some((tx, rx))
        } else {
            None
        };
        let mut dict_tx_opt = dict_channel.as_ref().map(|(tx, _)| Some(tx.clone()));
        let mut dict_rx_opt = dict_channel.map(|(_, rx)| Some(rx));

        let mut batch_chan_idx = 0usize;

        for config in stages.into_iter() {
            match config.kind {
                StageKind::ParquetSource(path) => {
                    // writes directly into batch_channels[0], no batcher needed!! also go GIL needed!
                    let sender = batch_senders[0].take().unwrap();
                    batch_chan_idx = 1;
                    handles.push(spawn_parquet_source(path, sender, batch_size));
                }

                StageKind::Source(cb) => {
                    let dict_tx = dict_tx_opt.as_mut().unwrap().take().unwrap();
                    let dict_rx = dict_rx_opt.as_mut().unwrap().take().unwrap();

                    handles.push(std::thread::spawn(move || {
                        let iter = Python::attach(|py| cb.call0(py).unwrap());
                        loop {
                            match Python::attach(|py| iter.call_method0(py, "__next__")) {
                                Ok(item) => { dict_tx.send(item).ok(); }
                                Err(_) => break,
                            }
                        }
                    }));

                    let batcher_tx = batch_senders[0].take().unwrap();
                    handles.push(spawn_batcher(dict_rx, batcher_tx, batch_size));
                    batch_chan_idx = 1;
                }

                StageKind::Stage(mut compute) => {
                    let receiver = batch_receivers[batch_chan_idx - 1].take().unwrap();
                    let sender = batch_senders[batch_chan_idx].take().unwrap();
                    batch_chan_idx += 1;

                    handles.push(std::thread::spawn(move || {
                        for batch in receiver.iter() {
                            let result = compute.process(batch);
                            sender.send(result).ok();
                        }
                    }));
                }

                StageKind::PyTransform(cb) => {
                    let receiver = batch_receivers[batch_chan_idx - 1].take().unwrap();
                    let sender = batch_senders[batch_chan_idx].take().unwrap();
                    batch_chan_idx += 1;

                    handles.push(std::thread::spawn(move || {
                        for batch in receiver.iter() {
                            Python::attach(|py| {
                                let py_batch = batch.to_pyarrow(py).unwrap();
                                let rows = py_batch.call_method0("to_pylist").unwrap();
                                let rows_list = rows.cast::<pyo3::types::PyList>().unwrap();

                                let results: Vec<Py<PyAny>> = rows_list.iter()
                                    .filter_map(|row| {
                                        let result = cb.call1(py, (row,)).ok()?;
                                        if result.is_none(py) { None } else { Some(result) }
                                    })
                                    .collect();

                                if !results.is_empty() {
                                    let pa = py.import("pyarrow").unwrap();
                                    let rb_class = pa.getattr("RecordBatch").unwrap();
                                    let pylist = pyo3::types::PyList::new(py, &results).unwrap();
                                    let new_batch: RecordBatch = RecordBatch::from_pyarrow_bound(
                                        &rb_class.call_method1("from_pylist", (pylist,)).unwrap()
                                    ).unwrap();
                                    sender.send(new_batch).ok();
                                }
                            });
                        }
                    }));
                }

                StageKind::ParquetSink(path) => {
                    // receives RecordBatches directly, writes to parquet - no GIL yaaay
                    let receiver = batch_receivers[batch_chan_idx - 1].take().unwrap();
                    handles.push(spawn_parquet_sink(path, receiver));
                }

                StageKind::Sink(cb) => {
                    let receiver = batch_receivers[batch_chan_idx - 1].take().unwrap();
                    handles.push(std::thread::spawn(move || {
                        for batch in receiver.iter() {
                            Python::attach(|py| {
                                let py_batch = batch.to_pyarrow(py).unwrap();
                                let rows = py_batch.call_method0("to_pylist").unwrap();
                                let rows_list = rows.cast::<pyo3::types::PyList>().unwrap();
                                for row in rows_list.iter() {
                                    cb.call1(py, (row,)).ok();
                                }
                            });
                        }
                    }));
                }
            }
        }

        py.detach(|| {
            for handle in handles {
                handle.join().unwrap();
            }
        });
    }
}