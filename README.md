# otters :otter:

Stream processing for Python. Built for prototyping.

Define a schema, wire up your transforms, point it at a data stream. Otters handles batching, backpressure, and shutdown. Rust handles the math.

```python
from otters import Pipeline, Schema
from otters.builtins import rolling_mean, zscore

schema = Schema({
    "symbol":    Schema.Str,
    "price":     Schema.F64,
    "volume":    Schema.I64,
    "timestamp": Schema.Timestamp("us"),
})

async def market_feed():
    async for tick in websocket.listen():
        yield tick  # plain dicts

p = Pipeline(schema=schema)
p.source(market_feed)
p.stage(rolling_mean(on="price", window=20))
p.stage(zscore(on="price", lookback=100))
p.stage(my_signal)   # just a function - dict in, dict out
p.sink("results.csv")
p.run()
```

No Kafka. No Flink. No infrastructure. Just a pipeline you can run anywhere Python runs.

## Who it's for

Quants, analysts, and engineers who want real streaming semantics while prototyping, without operating streaming infrastructure. When you're ready to scale, graduate to Flink or Bytewax. Otters gets you there faster.

## Sources and sinks

```python
p.source("trades.csv")                        
p.source("trades.parquet")
p.source(my_async_generator)                  

p.sink("results.csv")
p.sink("postgresql://localhost/db/results")
p.sink(print)                                 
p.sink(my_async_function)
```

## Stages

```python
# Builtin - Rust, runs on Arrow batches
p.stage(rolling_mean(on="price", window=20))
p.stage(rolling_std(on="price", window=20))
p.stage(zscore(on="price", lookback=100))
p.stage(ema(on="price", span=20))
p.stage(vwap(price="price", volume="volume", window=500))
p.stage(lag(on="price", periods=1))
p.stage(pct_change(on="price"))
p.stage(threshold(on="zscore", above=2.0, flag_as="is_anomaly"))

# Custom - plain Python
def my_signal(row: dict) -> dict:
    row["signal"] = 1 if row["zscore"] > 2.0 else 0
    return row

def drop_outliers(row: dict) -> dict | None:
    return None if row["price"] > 1_000_000 else row  # None drops the row

p.stage(my_signal)
p.stage(drop_outliers)
```

## How it works

Source yields dicts, otters batches them into Arrow RecordBatches, Rust stages operate on columns at native speed (no GIL, no FFI per row), results convert back to dicts for your sink.

You never touch Arrow directly.

## Plan

1. **Row-at-a-time pipeline**
   - crossbeam channels between stages
   - backpressure, shutdown cascading

2. **Arrow + batching**
   - auto-batcher: collect dicts → RecordBatches
   - auto-unbatcher: RecordBatches → dicts
   - channels carry batches instead of individual rows

3. **Rust compute stages**
   - builtin stats: rolling mean/std, zscore, ema, vwap, lag, pct_change
   - pure Rust on Arrow memory - no GIL, no per-row FFI

4. **Sources and sinks**
   - CSV, Parquet, JSON file sources
   - CSV, Parquet, Postgres sinks
   - Kafka source (streaming)

5. **Polish**
   - schema validation at construction time
   - graceful Ctrl+C shutdown
   - `p.describe()` - print the pipeline graph

## Built with

- [PyO3](https://pyo3.rs) - Rust/Python bindings
- [crossbeam-channel](https://docs.rs/crossbeam-channel) - bounded MPMC channels
- [arrow-rs](https://docs.rs/arrow) - Apache Arrow for Rust
- [maturin](https://maturin.rs) - build tool