import pyarrow as pa

class Schema:
    Str: str = "utf8"
    F64: str = "float64"
    I64: str = "int64"

    _TYPE_MAP: dict[str, pa.DataType] = {
        "utf8":    pa.utf8(),
        "float64": pa.float64(),
        "int64":   pa.int64(),
    }

    fields: dict[str, str]

    def __init__(self, fields: dict[str, str]) -> None:
        unknown = [typ for typ in fields.values() if typ not in self._TYPE_MAP]
        if unknown:
            raise ValueError(f"Unknown fields types: {unknown}. Use Schema types")
        
        self.fields = fields

    def to_arrow(self) -> pa.Schema:
        return pa.schema([
            pa.field(name, self._TYPE_MAP[dtype])
            for name, dtype in self.fields.items()
        ])