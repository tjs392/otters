from otters.schema import Schema
import pyarrow as pa

schema = Schema({
    "symbol":    Schema.Str,
    "price":     Schema.F64,
    "volume":    Schema.I64,
})

arrow_schema = schema.to_arrow()
print(arrow_schema)
# expected:
# symbol: string
# price: double
# volume: int64
# timestamp: timestamp[us]

# verify types came out right
assert arrow_schema.field("price").type == pa.float64()
assert arrow_schema.field("symbol").type == pa.utf8()
assert arrow_schema.field("volume").type == pa.int64()

print("schema ok")