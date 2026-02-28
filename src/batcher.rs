use pyo3::prelude::*;
use pyo3::types::PyList;
use crossbeam_channel::{Receiver, Sender};
use arrow::record_batch::RecordBatch;
use arrow::pyarrow::FromPyArrow;

pub struct Batcher {
    batch_size: usize,
    schema_fields: Vec<String>,
}

impl Batcher {
    pub fn new(batch_size: usize, schema_fields: Vec<String>) -> Self {
        Self { batch_size, schema_fields }
    }
}

/// spawn batcher thread
/// 
/// receives pytho dicts from source thread,
/// buffers them, and then flushes as Arrow RecordBatches
/// 
/// channel still carries PY<PyAny> on both sides
pub fn spawn_batcher(
    receiver: Receiver<Py<PyAny>>,
    sender: Sender<RecordBatch>,
    batch_size: usize,
) -> std::thread::JoinHandle<()> {

    // straight forward buffer batching stuff
    std::thread::spawn(move || {
        let mut buffer: Vec<Py<PyAny>> = Vec::with_capacity(batch_size);

        loop {
            match receiver.recv() {
                Ok(item) => buffer.push(item),
                Err(_) => {
                    if !buffer.is_empty() {
                        if let Some(batch) = flush(&buffer) {
                            sender.send(batch).ok();
                        }
                    }
                    break;
                }
            }

            while buffer.len() < batch_size {
                match receiver.try_recv() {
                    Ok(item) => buffer.push(item),
                    Err(_) => break,
                }
            }

            if buffer.len() >= batch_size {
                if let Some(batch) = flush(&buffer) {
                    sender.send(batch).ok();
                    buffer.clear();
                }
            }
        }
    })
}

///converts buffer of py dicts to single arrow recordbatch
/// aquires gil once per buffer/batch
fn flush(rows: &[Py<PyAny>]) -> Option<RecordBatch> {
    Python::attach(|py| {
        let lst = PyList::new(py, rows.iter().map(|r| r.bind(py))).ok()?;
        let pa = py.import("pyarrow").ok()?;
        let rb_class = pa.getattr("RecordBatch").ok()?;
        let py_batch = rb_class.call_method1("from_pylist", (lst,)).ok()?;
        RecordBatch::from_pyarrow_bound(&py_batch).ok()
    })
}