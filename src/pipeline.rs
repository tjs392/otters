use pyo3::prelude::*;

/// what role a stage plays in the pipeline
/// 
/// source - produces item from a python iterator
/// 
/// sink   - consumes items and calls a python callback, no output channel
/// 
/// stage  - receives items and transforms via python callback, sends results
enum StageKind {
    Source,
    Sink,
    Stage,
}

struct StageConfig {
    kind: StageKind,
    /// the python callable - either a generator from source or a transformation func
    callback: Py<PyAny>,
    workers: usize,
}

/// multi stage pipeline
/// 
/// register stages with source(), stage(), and sink()
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
    fn source(&mut self, callback: Py<PyAny>, workers: usize) {
        self.stages.push(StageConfig {
            kind: StageKind::Source,
            callback,
            workers,
        });
    }

    /// registers a sink stage
    fn sink(&mut self, callback: Py<PyAny>, workers: usize) {
        self.stages.push(StageConfig { 
            kind: StageKind::Sink, 
            callback, 
            workers,
        });
    }

    /// registers a transform stage
    fn stage(&mut self, callback: Py<PyAny>, workers: usize) {
        self.stages.push(StageConfig {
            kind: StageKind::Stage,
            callback,
            workers,
        })
    }

    /// wires up channels between stages, spawns workers threads, and
    /// blocks until the pipeline finishes
    /// 
    /// must give py so can release GIL while waiting
    fn run(&mut self, py: Python<'_>) {

        //create bounded channels, one between each pair of stages
        let mut senders = Vec::new();
        let mut receivers = Vec::new();

        for _ in (0..self.stages.len() - 1).rev() {
            let (s, r) = crossbeam_channel::bounded::<Py<PyAny>>(self.capacity);
            senders.push(s);
            receivers.push(r);
        }

        // take ownership by draining
        let stages: Vec<StageConfig> = self.stages.drain(..).collect();
        let mut handles = Vec::new();
        
        let batch_size = self.batch_size;

        for config in stages {
            let cb = config.callback;

            match config.kind {
                StageKind::Source => {
                    let sender = senders.pop().unwrap();
                    handles.push(std::thread::spawn(move || {
                        // call the python factory to get an iterator
                        // GIL is acquired for a second then release
                        let iter = Python::attach(|py| {
                            cb.call0(py).unwrap()
                        });

                        // here we acquire the GIL per item so other threads can interleave
                        // without this, an infinite source would starve other stages 
                        loop {
                            match Python::attach(|py| {
                                // __next__ is the python iterator protocol
                                iter.call_method0(py, "__next__")
                            }) {
                                Ok(item) => sender.send(item).unwrap(),
                                Err(_) => break,
                            }
                        }
                    }));
                }

                StageKind::Stage => {
                    let receiver = receivers.pop().unwrap();
                    let sender = senders.pop().unwrap();

                    for _ in 0..config.workers{
                        let rx = receiver.clone();
                        let tx = sender.clone();
                        let cb = Python::attach(|py| cb.clone_ref(py));
                        handles.push(std::thread::spawn(move || {
                            let mut batch = Vec::with_capacity(batch_size);
                            loop {
                                // recv() blocks without the GIL *****
                                // other threads can run python while this waits
                                match rx.recv() {
                                    Ok(item) => batch.push(item),
                                    Err(_) => break,
                                }

                                // drain any more that are ready, up to batch_size
                                while batch.len() < batch_size {
                                    match rx.try_recv() {
                                        Ok(item) => batch.push(item),
                                        Err(_) => break,
                                    }
                                }
                                
                                // once acquire GIL only for the transform allback
                                Python::attach(|py| {
                                    for item in batch.drain(..) {
                                        let result = cb.call1(py, (item,)).unwrap();
                                        tx.send(result).unwrap();
                                    }
                                });
                            }
                        }));
                    }

                    drop(receiver);
                    drop(sender);
                    drop(cb);
                }

                StageKind::Sink => {
                    let receiver = receivers.pop().unwrap();
                    handles.push(std::thread::spawn(move || {
                        let mut batch = Vec::with_capacity(batch_size);
                        loop {
                            match receiver.recv() {
                                Ok(item) => batch.push(item),
                                Err(_) => {
                                    if !batch.is_empty() {
                                        Python::attach(|py| {
                                            for item in batch.drain(..) {
                                                cb.call1(py, (item,)).unwrap();
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
                            Python::attach(|py| {
                                for item in batch.drain(..) {
                                    cb.call1(py, (item,)).unwrap();
                                }
                            });
                        }
                    }));
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