# otters :otter:

Streaming computation for large data. Built with rust + arrow + py03
Currently doing signal computation for tick data, but will expand on other use cases

The basic idea: pandas loads your entire dataset into memory before doing
anything. if you have a year of tick data thats probably not going to work
on your laptop. otters streams it in batches so memory usage stays flat
regardless of file size, and the compute stages run in parallel across threads.


---

## benchmarks

Currently I'm using my bad laptop, so i will update the benchmarks, but:
all benchmarks run on 5 signals (rolling_mean x2, zscore, ema, vwap)
and using a parquet to parquet compute pipeline.

The architecture will be built around using columnar data compute since the serialization overhead
of python dicts -> arrow is very costly and otters is not worth it in that case.

**Scaling with data size (parquet -> parquet)**

```
rows          file    pure python             pandas                 otters                 speedup
100,000        1MB    0.088s (1.1M rows/s)    0.075s (1.3M rows/s)   0.039s (2.5M rows/s)   1.89x
500,000        5MB    0.385s (1.3M rows/s)    0.195s (2.6M rows/s)   0.090s (5.6M rows/s)   2.17x
1,000,000     10MB    0.785s (1.3M rows/s)    0.269s (3.7M rows/s)   0.130s (7.7M rows/s)   2.06x
5,000,000     48MB    3.740s (1.3M rows/s)    1.349s (3.7M rows/s)   0.590s (8.5M rows/s)   2.29x
10,000,000    96MB    7.495s (1.3M rows/s)    2.854s (3.5M rows/s)   1.029s (9.7M rows/s)   2.77x
```

speedup grows with data size. at 10M rows otters is running at 9.7M rows/sec
while pandas sits at 3.5M. the gap keeps growing because pandas has to do
multiple sequential passes over the full dataset while otters pipelines
everything concurrently.

**memory usage**

```
pandas: +33 MB per 1M rows  (pandas scales linearly because it hsa to load entire file into RAM)
otters: ~0 MB               (constant mem usage since it  only holds batch_size rows at a time)

extrapolating:
  100M rows -> pandas needs ~3.3 GB RAM
  1B rows   -> pandas OOMs on basically everything
  otters    -> same memory usage regardless
```

The memory thing is the main reason I built this, kept running into OOM
errors trying to backtest on a full year of tick data with pandas. I thought it
would be a pretty cool project even if there were ways to get around it maybe with
polars or duckdb or some fancy pandas tricks. Been having fun with this though

---

## For running, i dont have this lib up yet soo gotta do it from source

```bash
# once oyu have the repo cloned and stuff
cd otters
maturin develop --release
# then you can import otters like a python lib
```

---

## Usage

**parquet -> signals -> parquet** (recommended, fastest path, no serialization overhead)
But I also support other forms as well as python callbacks, but the main use is to use the builtins with parquet files
I'm going to add psql interfaces though, and other columnar data interfaces

```python
import otters

p = otters.Pipeline(batch_size=10000)
p.source("trades.parquet")
p.rolling_mean("price", 20)
p.rolling_mean("price", 50)
p.zscore("price", 100)
p.ema("price", 20)
p.vwap("price", "volume", 50)
p.sink("signals.parquet")
p.run()
```

**live feed -> signals -> callback** 
this is python dict in and out say for websockets and other types of streaming or generators
this will be slower than raw optimized python due to the dict -> arrow serialzation overhead! be warned 

```python
def market_feed():
    for tick in websocket.listen():
        yield {"price": tick.price, "volume": tick.volume}

results = []
p = otters.Pipeline(batch_size=100)
p.source(market_feed)
p.rolling_mean("price", 20)
p.ema("price", 20)
p.sink(lambda row: results.append(row))
p.run()
```

---

## available signals

all signals are stateful across batches - state is maintained correctly even
when data arrives in chunks.

| signal | args | output column |
|---|---|---|
| `rolling_mean` | column, window | `{col}_rolling_mean_{window}` |
| `ema` | column, span | `{col}_ema_{span}` |
| `zscore` | column, lookback | `{col}_zscore_{lookback}` |
| `vwap` | price_col, volume_col, window | `vwap_{window}` |

---

## How it works

The pipeline spawns one thread per stage and connects them with bounded
crossbeam channels:

```
parquet reader -> [channel] -> rolling_mean -> [channel] -> zscore -> [channel] -> parquet writer
  thread 1                      thread 2                    thread 3                thread 4
```

While the writer is flushing batch N to disk, the compute stages are already
working on batch N+1 and N+2, and the reader is pulling in batch N+3. all
four things happen at the same time.

The channels are bounded so if a downstream
stage is slow the upstream stages block and wait instead of buffering
everything in memory. Thats the backpressure mechanism. Thank you crossbeam!

For parquet sources the data goes directly into arrow record batches in rust
and never touches python until the sink. This is why the parquet -> parquet
path is so much faster - zero GIL, zero dict -> arrow serialization overhead.

---

## Batch size

batch size controls how many rows are in each RecordBatch. bigger batches
mean better throughput, smaller batches mean lower latency.

Some quick tests, will vary on your machine and cache sizes
```
batch_size    throughput
100           ~4M rows/s
1000          ~8M rows/s
10000         ~9M rows/s
50000         ~8M rows/s      
```
You see this diminishing returns because the batch size gets greater than your caches... so vary for machine to machine

---

## When to use this vs pandas

Use otters when:
- Your data is in parquet files and doesnt fit in RAM
- You have multiple signals to compute (pipeline parallelism helps more)
- You have a live feed and need concurrent source/sink

Use pandas when:
- everything else lol

otters is not a pandas replacement. it does one thing: stream columnar data
through a sequence of compute stages without loading everything into memory.

---

## todo

- look into rust builtins for these calculations because some of them are hard especially with statefulness - want to reduce the points of failure
- postgress sink and source
- csv source and sink
- schema validation at pipeline consturction
- error handling >_>
- more builtins
- py_transform sucks right now... 
- async support