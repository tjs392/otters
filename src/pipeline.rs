use crossbeam_channel::{Receiver, Sender};
use pyo3::prelude::*;

/// what role a stage plays in the pipeline
/// 
/// source - produces item from a python iterator
/// 
/// sink   - consumes items and calls a python callback, no output channel
/// 
/// stage  - receives items and transforms via python callback, sends results
enum StageKind {
    Source(Py<PyAny>),
    Sink(Py<PyAny>),
    Stage(Compute),
}

/// the computation a middle stage performs
/// 
/// PyTransform lets users drop in plain python if they dont want to use a builtin
enum Compute {
    RollingMean { column: String, window: usize },
    ZScore { column: String, lookback: usize },
    Ema { column: String, span: usize },
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
    #[pyo3(signature = (capacity=1024, batch_size=1))]
    pub fn new(capacity: usize, batch_size: usize) -> Pipeline {
        Pipeline {
            stages: vec![],
            capacity,
            batch_size,
        }
    }

    /// registers a source stage
    /// example:
    ///   def source():
    ///       for row in csv.reader(f):
    ///           yield {"price": float(row[0]), ...}
    ///   p.source(source)
    fn source(&mut self, callback: Py<PyAny>) {
        self.stages.push(StageConfig {
            kind: StageKind::Source(callback),
        });
    }

    /// registers a sink stage
    /// example:
    ///   def sink(row):
    ///       db.insert(row)
    /// 
    ///   p.sink(sink)
    fn sink(&mut self, callback: Py<PyAny>) {
        self.stages.push(StageConfig { 
            kind: StageKind::Sink(callback),
        });
    }

    ////stages

    fn rolling_mean(&mut self, column: String, window: usize) {
        self.stages.push(StageConfig {
            kind: StageKind::Stage(Compute::RollingMean { column, window }),
        })
    }

    fn zscore(&mut self, column: String, lookback: usize) {
        self.stages.push(StageConfig {
            kind: StageKind::Stage(Compute::ZScore { column, lookback }),
        });
    }

    fn ema(&mut self, column: String, span: usize) {
        self.stages.push(StageConfig {
            kind: StageKind::Stage(Compute::Ema { column, span }),
        });
    }

    /// custom python transform
    fn py_transform(&mut self, callback: Py<PyAny>) {
        self.stages.push(StageConfig {
            kind: StageKind::Stage(Compute::PyTransform(callback)),
        });
    }

    /// wires up channels between stages, spawns workers threads, and
    /// blocks until the pipeline finishes
    /// 
    /// must give py so can release GIL while waiting
    fn run(&mut self, py: Python<'_>) {
        
        // take ownership by draining
        let stages: Vec<StageConfig> = self.stages.drain(..).collect();
        let mut handles = Vec::new();

        // need to copy primitives out of self so we dont borrow across threads
        let capacity = self.capacity;
        let batch_size = self.batch_size;

        let mut senders: Vec<Option<Sender<Py<PyAny>>>> = Vec::new();
        let mut receivers: Vec<Option<Receiver<Py<PyAny>>>> = Vec::new();

        let n = stages.len();
        for _ in 0..n - 1 {
            let (s, r) = crossbeam_channel::bounded(capacity);
            senders.push(Some(s));
            receivers.push(Some(r));
        }

        for (i, config) in stages.into_iter().enumerate() {
            match config.kind {
                StageKind::Source(cb) => {
                    // source should always be stage 0
                    let sender = senders[0].take().unwrap();
                    handles.push(std::thread::spawn(move || {
                        // call the python factory once to get the iter obj
                        // this aquires GIL for a sec, then releases it
                        let iter = Python::attach(|py| cb.call0(py).unwrap());
                        
                        // acquire GIL per item so other threads can run python between items.
                        // if we held the GIL across the whole loop an infinite source would
                        // starve every other stage that needs to call python.
                        loop {
                            match Python::attach(|py| iter.call_method0(py, "__next__")) {
                                Ok(item) => { sender.send(item).ok(); }
                                Err(_) => break,
                            }
                        }
                    }));
                }

                StageKind::Stage(compute) => {
                    let receiver = receivers[i - 1].take().unwrap();
                    let sender = senders[i].take().unwrap();

                    // GIL is not held here
                    // Py03 threads dont hold the GIL by default, they acquire it on demand
                    handles.push(std::thread::spawn(move || {
                        let mut batch = Vec::with_capacity(batch_size);
                        loop {
                            
                            // here just receive until we get to batch size
                            match receiver.recv() {
                                Ok(item) => batch.push(item),
                                Err(_) => break,
                            }

                            while batch.len() < batch_size {
                                match receiver.try_recv() {
                                    Ok(item) => batch.push(item),
                                    Err(_) => break,
                                }
                            }

                            match &compute {
                                Compute::RollingMean { column, window } => {
                                    // TODO: nothing for now just placeholder
                                    todo!("rolling mean not implemented yet")
                                }

                                // acquire gil and execute the python func if fed in
                                Compute::PyTransform(cb) => {
                                    Python::attach(|py| {
                                        for item in batch.drain(..) {
                                            let result = cb.call1(py, (item,)).unwrap();
                                            sender.send(result).ok();
                                        }
                                    });
                                }

                                _ => todo!("not implemented")
                            }
                        }
                    }));
                }

                StageKind::Sink(cb) => {
                    let receiver = receivers[i - 1].take().unwrap();
                    handles.push(std::thread::spawn(move || {
                        let mut batch = Vec::with_capacity(batch_size);
                        loop {
                            match receiver.recv() {
                                Ok(item) => batch.push(item),
                                Err(_) => {
                                    if !batch.is_empty() {
                                        Python::attach(|py| {
                                            for item in batch.drain(..) {
                                                cb.call1(py, (item,)).ok();
                                            }
                                        });
                                    }
                                    break;
                                }
                            }

                            while batch.len() < batch_size {
                                match receiver.try_recv() {
                                    Ok(item) => batch.push(item),
                                    Err(_) => break,
                                }
                            }

                            // flush the batch to python's sink callback
                            Python::attach(|py| {
                                for item in batch.drain(..) {
                                    cb.call1(py, (item,)).ok();
                                }
                            });
                        }
                    }))
                }
            }
        }

        // release the GIL from main thread while waiting for workers
        // without, spawn threads will never acquire the gil and we deadlock
        py.detach(|| {
            for handle in handles {
                handle.join().unwrap();
            }
        });
    }
}