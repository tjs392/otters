# otters :otter:

Stream processing for Python. Rust does the math.

Python handles IO (websockets, APIs, databases). Rust handles computation.
Data flows between them as Arrow batches through lock-free channels.
You never touch Arrow directly.
```python
from otters import Pipeline, rust_stage

p = Pipeline()
p.source(market_feed)
p.stage(rust_stage("vwap", window=500, key="symbol"))
p.stage(rust_stage("zscore", column="vwap", lookback=100))
p.sink(send_orders)
p.run()
```

Source yields Python dicts. Otters batches them into Arrow RecordBatches
automatically. Rust stages operate on Arrow columns at native speed - no
GIL, no FFI per item. Results convert back to dicts for the Python sink.

Python callbacks still work for lightweight stages. Rust plugins are for
the hot path.

## Plan

1. Row-at-a-time pipeline (done)
   - crossbeam channels between stages
   - multi-worker stages
   - backpressure, shutdown cascading

2. Arrow integration
   - add arrow-rs and pyarrow dependencies
   - auto-batcher: collect dicts into RecordBatches on the way in
   - auto-unbatcher: convert RecordBatches back to dicts on the way out
   - channels carry RecordBatches instead of Py<PyAny>

3. Rust-native compute stages
   - plugin registry: register Rust functions by name
   - stages take a RecordBatch, return a RecordBatch
   - ship with builtins: vwap, zscore, rolling mean, ema, etc.
   - no GIL, no FFI - pure Rust on Arrow memory

4. Production features
   - error handling (skip bad items, log errors)
   - graceful shutdown (ctrl+c)
   - key_by partitioning (route by symbol for stateful stages)
   - windowing (tumbling, sliding)

5. API polish
   - decorator syntax for Python stages
   - config objects for Rust stages
   - schema validation between stages

## Built with

- PyO3 - Rust/Python bindings
- crossbeam-channel - bounded MPMC channels
- arrow-rs - Apache Arrow for Rust
- maturin - build tool