use pyo3::prelude::*;

mod compute;
mod batcher;
mod builtins;
mod sources;
mod sinks;
mod pipeline;

#[pymodule]
fn otters(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<pipeline::Pipeline>()?;
    Ok(())
}