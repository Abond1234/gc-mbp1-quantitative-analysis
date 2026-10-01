"""Materialize a date-only split ledger from existing inventory/checkpoint metadata.

No event data is opened, no quality/outcome selection is performed, and no Bronze,
Silver, original audit or compatibility record is rewritten.
"""

from __future__ import annotations

import hashlib
import json
import sys
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.data.mbp1_paths import ROOT
from src.data.mbp1_splits import POLICY_ID, POLICY_SHA256, SPLITS, split_for_session


def build_assignments(rows: list[dict]) -> list[dict]:
    """Assign every inventoried weekday once, including empty source sessions."""
    dates = [row["session_date"] for row in rows]
    if len(dates) != len(set(dates)):
        raise ValueError("Duplicate source session dates")
    day = SPLITS["development"].start
    expected = set()
    while day <= SPLITS["final_test"].end:
        if day.weekday() < 5:
            expected.add(day.isoformat())
        day += timedelta(days=1)
    if set(dates) != expected:
        raise ValueError("Source dates differ from the frozen weekday coverage")
    assignments = []
    for row in sorted(rows, key=lambda r: r["session_date"]):
        if type(row["record_count"]) is not int or row["record_count"] < 0:
            raise ValueError("Invalid inventory record count")
        assignments.append({
            "session_date_ny": row["session_date"],
            "split": split_for_session(row["session_date"]),
            "record_count": row["record_count"],
            "raw_file": row["raw_file"],
            "raw_sha256": row["sha256"],
        })
    return assignments


def freeze_assignments() -> dict:
    inventory_path = ROOT / "data/metadata/mbp1/raw_inventory.json"
    inventory_bytes = inventory_path.read_bytes()
    assignments = build_assignments(json.loads(inventory_bytes))
    silver = ROOT / "data/processed/mbp1"
    catalog_bytes = (silver / "_SUCCESS.json").read_bytes()
    catalog = json.loads(catalog_bytes)
    silver_counts = {}
    for partition in catalog["partitions"]:
        checkpoint = json.loads((silver / partition["path"] / "_SUCCESS.json").read_text())
        if checkpoint["fingerprint"] != partition["fingerprint"]:
            raise ValueError("Published checkpoint identity mismatch")
        for session in checkpoint["sessions"]:
            day = session["session_date"]
            if day in silver_counts:
                raise ValueError("Duplicate Silver session")
            silver_counts[day] = session["record_count"]
    if silver_counts != {row["session_date_ny"]: row["record_count"] for row in assignments}:
        raise ValueError("Inventory and Silver session counts disagree")
    summary = []
    for split in SPLITS.values():
        selected = [row for row in assignments if row["split"] == split.name]
        summary.append({"split": split.name, "start": str(split.start), "end": str(split.end),
                        "source_sessions": len(selected),
                        "empty_sessions": sum(row["record_count"] == 0 for row in selected),
                        "records": sum(row["record_count"] for row in selected)})
    ledger = {"policy_id": POLICY_ID, "policy_sha256": POLICY_SHA256,
              "inventory_sha256": hashlib.sha256(inventory_bytes).hexdigest(),
              "catalog_sha256": hashlib.sha256(catalog_bytes).hexdigest(),
              "verification": "Existing inventory and checkpoint metadata only; no fresh event audit",
              "summary": summary, "sessions": assignments}
    destination = ROOT / "data/metadata/mbp1/research_splits" / POLICY_ID / "assignments.json"
    payload = json.dumps(ledger, indent=2, sort_keys=True) + "\n"
    if destination.exists():
        if destination.read_text(encoding="utf-8") != payload:
            raise ValueError("Frozen ledger differs; investigate and version the policy explicitly")
    else:
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open("x", encoding="utf-8", newline="\n") as stream:
            stream.write(payload)
    print(json.dumps({"ledger": str(destination), "summary": summary}, indent=2))
    return ledger


if __name__ == "__main__":
    freeze_assignments()
