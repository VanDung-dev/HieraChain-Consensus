"""
JSON to Arrow Data Transmission Tests.

Tests data integrity for serialization:
- JSON to Arrow IPC conversion
- Schema compatibility between Python and Rust
- Event serialization roundtrip
"""

import pytest
import json
import time
from typing import Any

# Python imports
try:
    import pyarrow as pa
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False

from hierachain.core.block import Block as PyBlock

# Rust imports
RUST_AVAILABLE = False
rust_calculate_merkle_root = None
rust_batch_calculate_hashes = None

try:
    import hierachain_consensus
    if hasattr(hierachain_consensus, "calculate_merkle_root"):
        rust_calculate_merkle_root = hierachain_consensus.calculate_merkle_root
    if hasattr(hierachain_consensus, "batch_calculate_hashes"):
        rust_batch_calculate_hashes = hierachain_consensus.batch_calculate_hashes
    RUST_AVAILABLE = True
except ImportError:
    pass


def create_events_list(count: int) -> list[dict[str, Any]]:
    """Create a list of test events."""
    return [
        {
            "entity_id": f"entity_{i}",
            "event": "json_arrow_test",
            "timestamp": 1000.0 + i,
            "details": {"index": i, "data": f"value_{i}"},
        }
        for i in range(count)
    ]


class TestJSONSerialization:
    """Test JSON serialization consistency."""

    def test_event_to_json_roundtrip(self):
        """Test event can be serialized and deserialized."""
        event = {
            "entity_id": "test_entity",
            "event": "created",
            "timestamp": 1000.5,
            "details": {"key": "value", "nested": {"inner": 123}},
        }

        # Serialize to JSON
        json_str = json.dumps(event, sort_keys=True)

        # Deserialize back
        restored = json.loads(json_str)

        assert restored == event

    def test_unicode_preservation(self):
        """Test unicode characters are preserved."""
        event = {
            "entity_id": "unicode_test",
            "event": "Tiếng Việt 中文 日本語",
            "timestamp": 1000.0,
        }

        json_str = json.dumps(event, ensure_ascii=False)
        restored = json.loads(json_str)

        assert restored["event"] == event["event"]

    def test_special_values(self):
        """Test handling of special JSON values."""
        event = {
            "entity_id": "special",
            "event": "test",
            "timestamp": 1000.0,
            "details": {
                "null_field": None,
                "bool_true": True,
                "bool_false": False,
                "integer": 42,
                "float": 3.14159,
            },
        }

        json_str = json.dumps(event)
        restored = json.loads(json_str)

        assert restored["details"]["null_field"] is None
        assert restored["details"]["bool_true"] is True
        assert restored["details"]["bool_false"] is False


@pytest.mark.skipif(not PYARROW_AVAILABLE, reason="PyArrow not available")
class TestArrowSerialization:
    """Test Arrow serialization."""

    def test_simple_schema_creation(self):
        """Test creating simple Arrow schema."""
        schema = pa.schema([
            ("entity_id", pa.string()),
            ("event", pa.string()),
            ("timestamp", pa.float64()),
        ])

        assert len(schema) == 3
        assert schema.field("entity_id").type == pa.string()

    def test_event_to_arrow_table(self):
        """Test converting events to Arrow table."""
        events = create_events_list(5)

        # Create arrays from events
        entity_ids = [e["entity_id"] for e in events]
        event_types = [e["event"] for e in events]
        timestamps = [e["timestamp"] for e in events]

        table = pa.table({
            "entity_id": entity_ids,
            "event": event_types,
            "timestamp": timestamps,
        })

        assert table.num_rows == 5
        assert table.num_columns == 3

    def test_arrow_ipc_roundtrip(self):
        """Test Arrow IPC serialization roundtrip."""
        events = create_events_list(10)

        entity_ids = [e["entity_id"] for e in events]
        timestamps = [e["timestamp"] for e in events]

        original_table = pa.table({
            "entity_id": entity_ids,
            "timestamp": timestamps,
        })

        # Serialize to IPC bytes
        sink = pa.BufferOutputStream()
        writer = pa.ipc.RecordBatchStreamWriter(sink, original_table.schema)
        writer.write_table(original_table)
        writer.close()

        ipc_bytes = sink.getvalue()

        # Deserialize back
        reader = pa.ipc.open_stream(ipc_bytes)
        restored_table = reader.read_all()

        assert restored_table.num_rows == original_table.num_rows
        assert restored_table.column_names == original_table.column_names


class TestHashConsistency:
    """Test hash calculation consistency."""

    def test_block_hash_format(self):
        """Test block hash is valid hex."""
        events = create_events_list(3)
        block = PyBlock(index=1, events=events)

        assert len(block.hash) == 64
        # Should be valid hex
        int(block.hash, 16)  # Raises ValueError if not hex

    @pytest.mark.skipif(not RUST_AVAILABLE or rust_calculate_merkle_root is None,
                        reason="Rust merkle root not available")
    def test_merkle_root_consistency(self):
        """Test Merkle root matches between Python and Rust."""
        events = create_events_list(5)

        # Python merkle via block
        py_block = PyBlock(index=1, events=events)
        py_merkle = py_block.merkle_root

        # Rust merkle
        rs_merkle = rust_calculate_merkle_root(events)

        assert py_merkle == rs_merkle

    @pytest.mark.skipif(not RUST_AVAILABLE or rust_batch_calculate_hashes is None,
                        reason="Rust batch hashes not available")
    def test_batch_hash_calculation(self):
        """Test batch hash calculation in Rust."""
        data_list = [
            {"id": 1, "value": "a"},
            {"id": 2, "value": "b"},
            {"id": 3, "value": "c"},
        ]

        hashes = rust_batch_calculate_hashes(data_list)

        assert len(hashes) == 3
        for h in hashes:
            assert len(h) == 64  # SHA256 hex


class TestCrossImplementationFormat:
    """Test format compatibility between Python and Rust."""

    def test_timestamp_format(self):
        """Test timestamp format consistency."""
        # Both should handle float timestamps
        ts = 1704067200.123456  # 2024-01-01 00:00:00.123456

        event = {
            "entity_id": "ts_test",
            "event": "test",
            "timestamp": ts,
        }

        # Should serialize without precision loss
        json_str = json.dumps(event)
        restored = json.loads(json_str)

        # Note: JSON may lose some precision
        assert abs(restored["timestamp"] - ts) < 0.0001

    def test_large_payload(self):
        """Test handling of larger payloads."""
        large_data = "x" * 10000  # 10KB string

        event = {
            "entity_id": "large_test",
            "event": "test",
            "timestamp": 1000.0,
            "data": large_data,
        }

        json_str = json.dumps(event)
        restored = json.loads(json_str)

        assert len(restored["data"]) == 10000


class TestPerformance:
    """Performance sanity checks."""

    def test_json_serialization_speed(self):
        """Test JSON serialization speed."""
        events = create_events_list(100)

        start = time.perf_counter()
        for event in events:
            json.dumps(event)
        elapsed = time.perf_counter() - start

        assert elapsed < 0.1, f"Too slow: {elapsed:.2f}s for 100 events"

    @pytest.mark.skipif(not PYARROW_AVAILABLE, reason="PyArrow needed")
    def test_arrow_creation_speed(self):
        """Test Arrow table creation speed."""
        events = create_events_list(1000)

        entity_ids = [e["entity_id"] for e in events]
        timestamps = [e["timestamp"] for e in events]

        start = time.perf_counter()
        for _ in range(10):
            pa.table({"entity_id": entity_ids, "timestamp": timestamps})
        elapsed = time.perf_counter() - start

        assert elapsed < 0.5, f"Too slow: {elapsed:.2f}s for 10 tables"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
