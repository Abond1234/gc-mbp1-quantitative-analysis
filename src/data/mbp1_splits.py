"""Frozen chronological research membership, independent of ingestion provenance."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, timedelta
from types import MappingProxyType

from src.data.mbp1_paths import ROOT

POLICY_PATH = ROOT / "configs/mbp1_research_splits.json"


def iso_date(value: str) -> date:
    """Accept only unambiguous YYYY-MM-DD dates, including at access boundaries."""
    parsed = date.fromisoformat(value)
    if value != parsed.isoformat():
        raise ValueError("Use canonical YYYY-MM-DD dates")
    return parsed


@dataclass(frozen=True)
class ResearchSplit:
    name: str
    start: date
    end: date


def parse_policy(policy: dict) -> dict[str, ResearchSplit]:
    """Reject gaps, overlaps, reordered roles and incompatible assignment rules."""
    if (policy["schema_version"] != 1 or policy["assignment_key"] != "session_date_ny"
            or policy["timezone"] != "America/New_York"):
        raise ValueError("Unsupported research split policy")
    names = ["development", "validation", "final_test"]
    if [row["name"] for row in policy["splits"]] != names:
        raise ValueError("Expected development, validation, final_test in chronological order")
    splits = {}
    next_start = iso_date(policy["coverage_start"])
    for row in policy["splits"]:
        split = ResearchSplit(row["name"], iso_date(row["start"]), iso_date(row["end"]))
        if split.start != next_start or split.end < split.start:
            raise ValueError("Research splits must be contiguous and non-overlapping")
        # Internal boundaries align with the canonical monthly Silver partitions.
        if splits and split.start.day != 1:
            raise ValueError("Holdouts must begin at a month boundary")
        splits[split.name] = split
        next_start = split.end + timedelta(days=1)
    if splits["final_test"].end != iso_date(policy["coverage_end"]):
        raise ValueError("Research splits must exhaust frozen coverage")
    return splits


_policy_bytes = POLICY_PATH.read_bytes()
_policy = json.loads(_policy_bytes)
POLICY_ID = _policy["policy_id"]
POLICY_SHA256 = hashlib.sha256(_policy_bytes).hexdigest()
SPLITS = MappingProxyType(parse_policy(_policy))


def split_for_session(session_date: str) -> str:
    day = iso_date(session_date)
    for split in SPLITS.values():
        if split.start <= day <= split.end:
            return split.name
    raise ValueError("Session lies outside frozen research coverage; review a new policy")


def authorize_range(start: str, end: str, *, allow_validation: bool = False,
                    allow_final_test: bool = False) -> tuple[date, date]:
    """Fail before file access; each intersected holdout needs its own opt-in."""
    if type(allow_validation) is not bool or type(allow_final_test) is not bool:
        raise TypeError("Holdout permissions must be explicit booleans")
    first, last = iso_date(start), iso_date(end)
    if first > last:
        raise ValueError("start must not exceed end")
    if first < SPLITS["development"].start or last > SPLITS["final_test"].end:
        raise ValueError("Requested range lies outside frozen research coverage")
    for name, allowed in (("validation", allow_validation), ("final_test", allow_final_test)):
        split = SPLITS[name]
        if first <= split.end and last >= split.start and not allowed:
            raise PermissionError(
                f"{name} ({split.start} through {split.end}) is held out; "
                f"explicit allow_{name}=True requires a separately authorized procedure"
            )
    return first, last
