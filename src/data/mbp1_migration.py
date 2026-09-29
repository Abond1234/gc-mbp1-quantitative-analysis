"""Narrow, explicit compatibility for checkpoints built before path migration.

No checksum or source-field check is skipped. Only an exact reviewed runtime
configuration may recompute the original fingerprint with the original code
identity. Different source bytes, timestamps, dependencies or incoming roll
state still produce a different fingerprint. The original checkpoint remains
unchanged; the current runtime identity is recorded in new run provenance.
"""

import hashlib
import json

from src.data.mbp1_paths import ROOT

COMPATIBILITY = ROOT / "configs/mbp1_migration_compatibility.json"


def migration_fingerprint(config: dict, rows: list[dict], state: dict) -> str | None:
    if not COMPATIBILITY.is_file():
        return None
    record = json.loads(COMPATIBILITY.read_text(encoding="utf-8"))
    if record.get("schema_version") != 1 or config != record["reviewed_runtime_config"]:
        return None
    payload = {"config": record["original_build_config"], "sources": rows,
               "incoming_state": state}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()
