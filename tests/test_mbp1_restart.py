"""Real synthetic DBN/Zstandard files exercise corruption and checkpoint handling."""

import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import databento_dbn as dbn
import pyarrow.parquet as pq
import zstandard as zstd

from src.data.mbp1_ingest import convert_month, inspect_source
from src.data.mbp1_validation import session_bounds
from tests.test_mbp1_foundation import DTYPE, sample


def synthetic_file(root: Path, day: str, instrument=100, empty=False):
    start, end = session_bounds(day)
    metadata = dbn.Metadata(
        dataset="GLBX.MDP3", schema=dbn.Schema.MBP_1,
        stype_in=dbn.SType.CONTINUOUS, stype_out=dbn.SType.INSTRUMENT_ID,
        start=start, end=end, symbols=["GC.v.0"], version=3,
        mappings=[SimpleNamespace(raw_symbol="GC.v.0", intervals=[SimpleNamespace(
            start_date=date.fromisoformat(day),
            end_date=date.fromisoformat(day) + timedelta(days=1), symbol=str(instrument))])],
    )
    a = sample(0 if empty else 6, day)
    a["instrument_id"] = instrument
    folder = root / day[:4]
    folder.mkdir(exist_ok=True)
    path = folder / f"GC.v.0_{day}_0700-1200_NY_mbp-1.dbn.zst"
    path.write_bytes(zstd.ZstdCompressor().compress(metadata.encode() + a.tobytes()))
    Path(str(path) + ".done").write_text("synthetic test source")
    return path


class MBP1RestartTests(unittest.TestCase):
    def test_inventory_detects_truncated_frame_and_missing_marker(self):
        with tempfile.TemporaryDirectory() as temp:
            path = synthetic_file(Path(temp), "2024-09-02")
            row, dtype = inspect_source(path, DTYPE)
            self.assertEqual(row["record_count"], 6)
            self.assertTrue(row["readable"])
            path.write_bytes(path.read_bytes()[:-1])
            row, _ = inspect_source(path, dtype)
            self.assertIn("Incomplete Zstandard", row["error"])
            Path(str(path) + ".done").unlink()
            row, _ = inspect_source(path, dtype)
            self.assertIn("Missing completion marker", row["error"])

    def test_empty_source_and_atomic_checkpoint_reuse(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            paths = [synthetic_file(root, "2024-09-02"),
                     synthetic_file(root, "2024-09-03", empty=True)]
            rows = [inspect_source(path, DTYPE)[0] for path in paths]
            self.assertEqual(rows[1]["record_count"], 0)
            silver = root / "silver"
            state = {"last_id": None, "roll_segment": 1}
            with patch("src.data.mbp1_ingest.CHUNK_ROWS", 3), patch("src.data.mbp1_ingest.PART_ROWS", 5):
                result = convert_month("2024-09", rows, DTYPE, state, {"test": 1}, silver)
                self.assertEqual(result["reconciliation"]["records"], 6)
                self.assertEqual(result["sessions"][1]["record_count"], 0)
                part = silver / result["path"] / result["files"][0]["name"]
                timestamp = part.stat().st_mtime_ns
                again = convert_month("2024-09", rows, DTYPE,
                    {"last_id": None, "roll_segment": 1}, {"test": 1}, silver)
                self.assertEqual(again["fingerprint"], result["fingerprint"])
                self.assertEqual(part.stat().st_mtime_ns, timestamp)
                self.assertEqual(pq.ParquetFile(part).metadata.num_rows, 6)
                part.write_bytes(part.read_bytes() + b"corruption")
                with self.assertRaisesRegex(ValueError, "modified"):
                    convert_month("2024-09", rows, DTYPE,
                        {"last_id": None, "roll_segment": 1}, {"test": 1}, silver)


if __name__ == "__main__":
    unittest.main()
