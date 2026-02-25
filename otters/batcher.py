import time
import pyarrow as pa

from .schema import Schema


class Batcher:
    _schema: pa.Schema
    _buffer: list[dict]
    _batch_size: int
    _flush_interval: float
    _last_flush: float

    def __init__(self, schema: Schema, batch_size: int = 500, flush_ms: int = 50) -> None:
        if batch_size < 1:
            raise ValueError(f"batch_size must be >= 1, got {batch_size}")
        if flush_ms < 1:
            raise ValueError(f"flush_ms must be >= 1, got {flush_ms}")
        
        self._schema = schema.to_arrow()
        self._buffer = []
        self._batch_size = batch_size
        self._flush_interval = flush_ms / 1000.0
        self._last_flush = time.monotonic()
    
    def push(self, row: dict) -> pa.RecordBatch | None:
        self._buffer.append(row)

        if len(self._buffer) >= self._batch_size:
            return self._flush()

        if time.monotonic() - self._last_flush >= self._flush_interval:
            return self._flush()

        return None

    def flush_remaining(self) -> pa.RecordBatch | None:
        return self._flush()
    
    def _flush(self) -> pa.RecordBatch | None:
        if not self._buffer:
            return None

        batch = pa.RecordBatch.from_pylist(self._buffer, schema=self._schema)
        self._buffer.clear()
        self._last_flush = time.monotonic()
        return batch



