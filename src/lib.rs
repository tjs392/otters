use pyo3::prelude::*;
mod pipeline;

#[pymodule(gil_used = false)]
mod otters {
    use pyo3::prelude::*;
    use crate::pipeline::Pipeline;

    #[pymodule_init]
    fn init(m: &Bound<'_, PyModule>) -> PyResult<()> {
        m.add_class::<Pipeline>()?;
        Ok(())
    }
}