from otters.schema import Schema
from otters.batcher import Batcher

schema = Schema({"symbol": Schema.Str, "price": Schema.F64})
batcher = Batcher(schema=schema, batch_size=3, flush_ms=10000)

batcher.push({"symbol": "AAPL", "price": 150.0})
batcher.push({"symbol": "AAPL", "price": 152.0})
batch = batcher.push({"symbol": "AAPL", "price": 148.0})

assert batch is not None
assert batch.num_rows == 3
assert batch.column("symbol").to_pylist() == ["AAPL", "AAPL", "AAPL"]
assert batch.column("price").to_pylist() == [150.0, 152.0, 148.0]

print("batch ok")