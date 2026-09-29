"""Integrity regressions using synthetic records; no market-data dependencies."""

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import databento_dbn as dbn
import numpy as np
import pyarrow.parquet as pq

from src.data.mbp1_access import scan_mbp1
from src.data.mbp1_ingest import arrow_to_native, native_to_arrow, reconcile_partition
from src.data.mbp1_validation import SessionAudit, session_bounds

DTYPE = np.dtype([
    ("length", "u1"), ("rtype", "u1"), ("publisher_id", "u2"), ("instrument_id", "u4"),
    ("ts_event", "u8"), ("price", "i8"), ("size", "u4"), ("action", "S1"),
    ("side", "S1"), ("flags", "u1"), ("depth", "u1"), ("ts_recv", "u8"),
    ("ts_in_delta", "i4"), ("sequence", "u4"), ("bid_px_00", "i8"),
    ("ask_px_00", "i8"), ("bid_sz_00", "u4"), ("ask_sz_00", "u4"),
    ("bid_ct_00", "u4"), ("ask_ct_00", "u4"),
])


def sample(n=6, day="2024-09-02"):
    a = np.zeros(n, dtype=DTYPE)
    a["length"], a["rtype"], a["publisher_id"] = 20, 1, 1
    a["instrument_id"] = 100
    a["ts_recv"] = session_bounds(day)[0] + np.arange(n, dtype=np.uint64) + 100
    a["ts_event"] = a["ts_recv"] - 10
    a["price"], a["bid_px_00"], a["ask_px_00"] = 2_500_100_000_000, 2_500_000_000_000, 2_500_100_000_000
    a["size"], a["bid_sz_00"], a["ask_sz_00"] = 1, 1, 1
    a["bid_ct_00"], a["ask_ct_00"] = 1, 1
    a["action"], a["side"], a["flags"] = b"T", b"B", 128
    a["sequence"] = np.arange(n)
    return a


class MBP1FoundationTests(unittest.TestCase):
    def test_dst_window(self):
        winter, summer = session_bounds("2024-01-08"), session_bounds("2024-07-08")
        for start, end in (winter, summer):
            self.assertEqual(end - start, 18_000 * 10**9)
        self.assertEqual((winter[0] // 10**9) % 86400, 12 * 3600)
        self.assertEqual((summer[0] // 10**9) % 86400, 11 * 3600)

    def test_native_precision_sentinels_and_roll_boundary(self):
        a = sample()
        a["price"][0] = dbn.UNDEF_PRICE
        a["ts_event"][1] = dbn.UNDEF_TIMESTAMP
        a["instrument_id"][3:] = 200
        state = {"last_id": 99, "roll_segment": 3}
        table = native_to_arrow(a, "2024-09-02", 8, state)
        np.testing.assert_array_equal(a, arrow_to_native(table, DTYPE))
        self.assertEqual(table["roll_segment"].to_pylist(), [4, 4, 4, 5, 5, 5])
        self.assertEqual(table["event_idx_day"].to_pylist(), list(range(8, 14)))
        self.assertEqual(table["ts_event"].null_count, 1)
        self.assertEqual(state, {"last_id": 200, "roll_segment": 5})

    def test_duplicates_nonadjacent_and_chunk_boundary(self):
        a = sample()
        a["ts_recv"] = a["ts_recv"][0]
        a[3], a[4], a[5] = a[0], a[1], a[0]
        audit = SessionAudit("2024-09-02", {100})
        audit.consume(a[:3])
        audit.consume(a[3:5])
        audit.consume(a[5:])
        self.assertEqual(audit.result()["checks"]["exact_duplicates"], 3)
        self.assertTrue(audit.result()["duplicate_coverage_complete"])

    def test_anomalies_do_not_clean(self):
        a = sample()
        a["ts_recv"][3] -= np.uint64(2)
        a["bid_px_00"][2] = a["ask_px_00"][2] + 100_000_000
        a["flags"][2] = 0
        a["bid_px_00"][4] = dbn.UNDEF_PRICE
        a["bid_sz_00"][4] = a["bid_ct_00"][4] = 0
        original = a.copy()
        audit = SessionAudit("2024-09-02", {100})
        audit.consume(a[:3])
        audit.consume(a[3:])
        result = audit.result()
        self.assertFalse(result["duplicate_coverage_complete"])
        self.assertEqual(result["checks"]["crossed_bbo_partial"], 1)
        self.assertEqual(result["checks"]["crossed_bbo_last"], 0)
        self.assertEqual(result["checks"]["quote_size_count_mismatch"], 0)
        np.testing.assert_array_equal(original, a)

    def test_readback_detects_field_corruption_and_query_prunes(self):
        a = sample()
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            folder = root / "year=2024" / "month=09"
            folder.mkdir(parents=True)
            path = folder / "part-0000.parquet"
            table = native_to_arrow(a, "2024-09-02", 0, {"last_id": None, "roll_segment": 1})
            pq.write_table(table, path)
            files = [{"name": path.name, "rows": len(a), "bytes": path.stat().st_size}]
            sessions = [{"session_date": "2024-09-02", "record_count": len(a),
                         "native_sha256": hashlib.sha256(a.tobytes()).hexdigest()}]
            self.assertEqual(reconcile_partition(folder, files, sessions, DTYPE)["records"], 6)
            (folder / "_SUCCESS.json").write_text(json.dumps({"fingerprint": "x", "files": files}))
            (root / "_SUCCESS.json").write_text(json.dumps({"partitions": [
                {"month": "2024-09", "path": "year=2024/month=09", "fingerprint": "x"}]}))
            scanner = scan_mbp1("2024-09-02", "2024-09-02", ["price"], root=root)
            self.assertEqual(scanner.to_table().column_names, ["price"])
            self.assertEqual(scan_mbp1("2024-09-03", "2024-09-03", root=root).count_rows(), 0)
            a["price"][2] += 1
            bad = native_to_arrow(a, "2024-09-02", 0, {"last_id": None, "roll_segment": 1})
            pq.write_table(bad, path)
            with self.assertRaisesRegex(ValueError, "native-byte"):
                reconcile_partition(folder, files, sessions, DTYPE)

    def test_unpublished_store_and_final_test_fail_closed(self):
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaises(FileNotFoundError):
                scan_mbp1("2024-01-01", "2024-01-02", root=temp)
            with self.assertRaises(PermissionError):
                scan_mbp1("2025-01-01", "2025-01-02", root=temp)


if __name__ == "__main__":
    unittest.main()
