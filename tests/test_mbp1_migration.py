"""Migration compatibility must preserve the original integrity checks."""

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.data.mbp1_ingest import convert_month, inspect_source
from src.data.mbp1_migration import migration_fingerprint
from tests.test_mbp1_foundation import DTYPE
from tests.test_mbp1_restart import synthetic_file


class MigrationTests(unittest.TestCase):
    def test_compatibility_checks_config_source_and_roll_state(self):
        original = {"code_sha256": {"engine": "old"}, "libraries": {"numpy": "pinned"}}
        current = {"code_sha256": {"engine": "new"}, "libraries": {"numpy": "pinned"}}
        rows = [{"sha256": "source", "mtime_ns": 42}]
        state = {"last_id": 123, "roll_segment": 2}
        expected = hashlib.sha256(json.dumps(
            {"config": original, "sources": rows, "incoming_state": state},
            sort_keys=True, default=str).encode()).hexdigest()
        with tempfile.TemporaryDirectory() as temp:
            record = Path(temp) / "compatibility.json"
            record.write_text(json.dumps({"schema_version": 1,
                "original_build_config": original, "reviewed_runtime_config": current}))
            with patch("src.data.mbp1_migration.COMPATIBILITY", record):
                self.assertEqual(migration_fingerprint(current, rows, state), expected)
                for changed in ({**current, "libraries": {"numpy": "different"}},
                                {**current, "code_sha256": {"engine": "unreviewed"}}):
                    self.assertIsNone(migration_fingerprint(changed, rows, state))
                self.assertNotEqual(migration_fingerprint(current, [{**rows[0], "sha256": "changed"}], state), expected)
                self.assertNotEqual(migration_fingerprint(current, [{**rows[0], "mtime_ns": 43}], state), expected)
                self.assertNotEqual(migration_fingerprint(current, rows, {**state, "last_id": 999}), expected)

    def test_relocated_checkpoint_reuse_and_corruption_fail_closed(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            path = synthetic_file(root, "2024-09-02")
            rows = [inspect_source(path, DTYPE)[0]]
            state = {"last_id": None, "roll_segment": 1}
            original, current = {"build": "original"}, {"build": "reviewed-migration"}
            silver = root / "silver"
            checkpoint = convert_month("2024-09", rows, DTYPE, dict(state), original, silver)
            part = silver / checkpoint["path"] / checkpoint["files"][0]["name"]
            before = part.stat().st_mtime_ns
            record = root / "compatibility.json"
            record.write_text(json.dumps({"schema_version": 1,
                "original_build_config": original, "reviewed_runtime_config": current}))
            with patch("src.data.mbp1_migration.COMPATIBILITY", record):
                result = convert_month("2024-09", rows, DTYPE, dict(state), current, silver, reuse_only=True)
                self.assertEqual(result["fingerprint"], checkpoint["fingerprint"])
                self.assertEqual(part.stat().st_mtime_ns, before)
                with self.assertRaisesRegex(ValueError, "Reuse-only"):
                    convert_month("2024-09", rows, DTYPE, dict(state), {"build": "changed"}, silver, reuse_only=True)
                content = bytearray(part.read_bytes())
                content[len(content) // 2] ^= 1
                part.write_bytes(content)
                with self.assertRaisesRegex(ValueError, "modified"):
                    convert_month("2024-09", rows, DTYPE, dict(state), current, silver, reuse_only=True)

    def test_reuse_only_never_creates_missing_partition(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            with self.assertRaisesRegex(ValueError, "Reuse-only"):
                convert_month("2024-09", [], DTYPE, {"last_id": None, "roll_segment": 1}, {}, root, reuse_only=True)
            self.assertEqual(list(root.iterdir()), [])
