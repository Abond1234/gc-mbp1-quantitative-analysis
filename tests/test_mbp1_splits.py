"""Research leakage regressions using synthetic stores only."""

import copy
import json
import tempfile
import unittest
from collections import Counter
from datetime import date, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from scripts.freeze_mbp1_research_splits import build_assignments
from src.data.mbp1_access import mbp1_files, scan_development, scan_mbp1
from src.data.mbp1_splits import POLICY_PATH, SPLITS, parse_policy, split_for_session


class ResearchSplitTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.days = ["2021-09-27", "2024-09-27", "2024-09-30", "2024-10-01",
                     "2025-09-30", "2025-10-01", "2026-09-25"]
        partitions = []
        for month in sorted({day[:7] for day in self.days}):
            folder = self.root / f"year={month[:4]}/month={month[5:]}"
            folder.mkdir(parents=True)
            # Equal native prices and nonzero indices are deliberately retained.
            records = [{"session_date_ny": date.fromisoformat(day), "event_idx_day": i,
                        "price": 2_500_100_000_001}
                       for day in self.days if day.startswith(month) for i in (0, 1)]
            path = folder / "part-0000.parquet"
            pq.write_table(pa.Table.from_pylist(records), path, row_group_size=2,
                           write_page_checksum=True)
            checkpoint = {"fingerprint": month, "files": [
                {"name": path.name, "bytes": path.stat().st_size, "rows": len(records)}]}
            (folder / "_SUCCESS.json").write_text(json.dumps(checkpoint))
            partitions.append({"month": month, "path": folder.relative_to(self.root).as_posix(),
                               "fingerprint": month})
        (self.root / "_SUCCESS.json").write_text(json.dumps({"partitions": partitions}))

    def test_development_defaults_and_exact_last_day(self):
        table = scan_development(root=self.root, batch_size=1).to_table()
        expected = [date.fromisoformat(day) for day in self.days[:3] for _ in (0, 1)]
        self.assertEqual(table["session_date_ny"].to_pylist(), expected)
        self.assertEqual(table["event_idx_day"].to_pylist(), [0, 1] * 3)
        self.assertEqual(table["price"].to_pylist(), [2_500_100_000_001] * 6)
        selected = scan_development("2024-09-30", "2024-09-30", ["price"], root=self.root)
        self.assertEqual(selected.to_table().column_names, ["price"])
        self.assertEqual(selected.count_rows(), 2)
        files = mbp1_files("2021-09-27", "2024-09-30", self.root)
        self.assertEqual(len(files), 2)

    def test_holdout_boundaries_and_crossing_requests_fail_before_io(self):
        missing = self.root / "missing"
        for start, end in [("2024-10-01", "2024-10-01"),
                           ("2025-09-30", "2025-09-30"),
                           ("2025-10-01", "2025-10-01"),
                           ("2026-09-25", "2026-09-25"),
                           ("2024-09-30", "2024-10-01"),
                           ("2025-09-30", "2025-10-01"),
                           ("2021-09-27", "2026-09-25")]:
            for reader in (scan_mbp1, scan_development, mbp1_files):
                with self.subTest(start=start, end=end, reader=reader.__name__):
                    with self.assertRaises(PermissionError):
                        reader(start, end, root=missing)

    def test_holdout_opt_ins_are_independent_and_filter_exact_dates(self):
        for start, end, flags, expected in [
            ("2024-10-01", "2025-09-30", {"allow_validation": True}, 4),
            ("2025-10-01", "2026-09-25", {"allow_final_test": True}, 4),
            ("2024-09-30", "2024-10-01", {"allow_validation": True}, 4),
            ("2021-09-27", "2026-09-25",
             {"allow_validation": True, "allow_final_test": True}, 14),
        ]:
            with self.subTest(flags=flags, start=start):
                table = scan_mbp1(start, end, root=self.root, **flags).to_table()
                self.assertEqual(table.num_rows, expected)
                self.assertTrue(all(start <= str(day) <= end
                                    for day in table["session_date_ny"].to_pylist()))
        with self.assertRaises(PermissionError):
            scan_mbp1("2024-10-01", "2024-10-01", root=self.root, allow_final_test=True)
        with self.assertRaises(PermissionError):
            scan_mbp1("2025-10-01", "2025-10-01", root=self.root, allow_validation=True)
        with self.assertRaises(PermissionError):
            scan_mbp1("2025-09-30", "2025-10-01", root=self.root, allow_final_test=True)
        with self.assertRaises(TypeError):
            scan_development("2025-10-01", "2025-10-01", allow_final_test=True)

    def test_bad_dates_future_data_and_truthy_strings_fail_closed(self):
        for start, end in [("2024-09-30", "2024-09-01"),
                           ("20240930", "20240930"),
                           ("2024-W40-1", "2024-W40-1"),
                           ("2024-09-31", "2024-09-31"),
                           ("2021-09-26", "2021-09-27"),
                           ("2026-09-25", "2026-09-26")]:
            with self.subTest(start=start, end=end), self.assertRaises(ValueError):
                scan_mbp1(start, end, root=self.root,
                           allow_validation=True, allow_final_test=True)
        with self.assertRaises(TypeError):
            scan_mbp1("2024-10-01", "2024-10-01", root=self.root, allow_validation="false")

    def test_policy_rejects_overlaps_gaps_reordering_and_partial_months(self):
        policy = json.loads(POLICY_PATH.read_text())
        for start in ("2024-09-30", "2024-10-02"):
            changed = copy.deepcopy(policy)
            changed["splits"][1]["start"] = start
            with self.assertRaises(ValueError):
                parse_policy(changed)
        changed = copy.deepcopy(policy)
        changed["splits"].reverse()
        with self.assertRaises(ValueError):
            parse_policy(changed)
        changed = copy.deepcopy(policy)
        changed["splits"][0]["end"] = "2024-09-29"
        changed["splits"][1]["start"] = "2024-09-30"
        with self.assertRaises(ValueError):
            parse_policy(changed)
        for name, split in SPLITS.items():
            self.assertEqual(split_for_session(str(split.start)), name)
            self.assertEqual(split_for_session(str(split.end)), name)
        with self.assertRaises(ValueError):
            split_for_session("2026-09-26")

    def test_every_weekday_assigned_once_including_empty_sources(self):
        rows = []
        day = date(2021, 9, 27)
        while day <= date(2026, 9, 25):
            if day.weekday() < 5:
                rows.append({"session_date": str(day), "record_count": 0,
                             "raw_file": f"synthetic/{day}", "sha256": "synthetic"})
            day += timedelta(days=1)
        result = build_assignments(rows[::-1])
        self.assertEqual(len(result), 1305)
        self.assertEqual(Counter(row["split"] for row in result),
                         {"development": 786, "validation": 261, "final_test": 258})
        self.assertEqual(sum(row["record_count"] for row in result), 0)
        self.assertEqual(result[0]["session_date_ny"], "2021-09-27")
        for bad in (rows[:-1], rows + [rows[0]], rows + [
                {**rows[-1], "session_date": "2026-09-28"}]):
            with self.assertRaises(ValueError):
                build_assignments(bad)


if __name__ == "__main__":
    unittest.main()
